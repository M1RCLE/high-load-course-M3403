package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpClient.Version
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()

        // Флаг для включения/выключения hedge-запросов
        const val HEDGE_ENABLED = false
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val httpClientExecutor = ThreadPoolExecutor(
        64, 64, 0, TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(100_000),
        NamedThreadFactory("payment-http-client")
    )

    private val dbExecutor = ThreadPoolExecutor(
        1000, 1000, 0, TimeUnit.SECONDS,
        LinkedBlockingQueue(50_000),
        NamedThreadFactory("payment-db-callback")
    )

    // Используется только если HEDGE_ENABLED = true
    private val scheduler = Executors.newScheduledThreadPool(8, NamedThreadFactory("payment-hedge-scheduler"))

    private val client = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .executor(httpClientExecutor)
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        (rateLimitPerSec * 0.95).toLong(),
        Duration.ofSeconds(1)
    )

    private val circuitBreaker = CircuitBreaker.of(
        "payment-cb-$accountName",
        CircuitBreakerConfig.custom()
            .slidingWindowType(SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(300)
            .failureRateThreshold(5f)
            .slowCallRateThreshold(20f)
            .slowCallDurationThreshold(averageProcessTime.multipliedBy(2))
            .minimumNumberOfCalls(50)
            .waitDurationInOpenState(Duration.ofSeconds(20))
            .permittedNumberOfCallsInHalfOpenState(5)
            .build()
    ).also { cb ->
        cb.eventPublisher.onStateTransition { event ->
            logger.warn("[$accountName] Circuit breaker state: ${event.stateTransition.fromState} → ${event.stateTransition.toState}")
        }
        cb.eventPublisher.onFailureRateExceeded { event ->
            logger.error("[$accountName] Failure rate exceeded: ${event.failureRate}%")
        }
        cb.eventPublisher.onSlowCallRateExceeded { event ->
            logger.warn("[$accountName] Slow call rate exceeded: ${event.slowCallRate}%")
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn("[$accountName] Circuit breaker is OPEN. Fast-failing payment $paymentId (state: ${circuitBreaker.state})")
            paymentESService.update(paymentId) {
                it.logProcessing(
                    success = false,
                    processedAt = now(),
                    transactionId = transactionId,
                    reason = "Circuit breaker OPEN: external service is unhealthy"
                )
            }
            return
        }

        rateLimiter.tickBlocking()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId = transactionId,
                startedAt = now(),
                spentInQueueDuration = Duration.ofMillis(now() - paymentStartedAt)
            )
        }

        val callStartedAt = now()

        val future = if (HEDGE_ENABLED) {
            performHedgedPayment(paymentId, amount, transactionId, deadline)
        } else {
            performSinglePayment(paymentId, amount, transactionId)
        }

        future
            .thenAccept { success ->
                val duration = now() - callStartedAt
                if (success) {
                    circuitBreaker.onSuccess(duration, TimeUnit.MILLISECONDS)
                } else {
                    circuitBreaker.onError(
                        duration, TimeUnit.MILLISECONDS,
                        PaymentDeclinedException("Payment $paymentId was declined")
                    )
                }
            }
            .exceptionally { exception ->
                val duration = now() - callStartedAt
                circuitBreaker.onError(duration, TimeUnit.MILLISECONDS, exception)
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", exception)
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        success = false,
                        processedAt = now(),
                        transactionId = transactionId,
                        reason = exception.message ?: "Unknown error"
                    )
                }
                null
            }
    }

    /**
     * Простой одиночный запрос без hedge.
     * Включается при HEDGE_ENABLED = false.
     */
    private fun performSinglePayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
    ): CompletableFuture<Boolean> {
        return sendSingleRequest(paymentId, amount, transactionId)
            .thenApplyAsync({ (success, reason) ->
                paymentESService.update(paymentId) {
                    it.logProcessing(success, now(), transactionId, reason = reason)
                }
                success
            }, dbExecutor)
    }

    /**
     * Отправляет первый запрос. если за averageProcessTime не получен ответ —
     * отправляет параллельный (hedge) запрос с тем же transactionId (идемпотентность)
     *
     * В состоянии HALF_OPEN хедж отключён: мы намеренно ограничиваем нагрузку
     * на восстанавливающийся сервис и хотим чистый сигнал от одного запроса
     *
     * Включается при HEDGE_ENABLED = true
     */
    private fun performHedgedPayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): CompletableFuture<Boolean> {
        val result = CompletableFuture<Boolean>()
        val pending = AtomicInteger(0)

        fun onSuccess(success: Boolean, reason: String?) {
            if (result.complete(success)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(success, now(), transactionId, reason = reason)
                }
            }
        }

        fun onError(ex: Throwable) {
            if (pending.decrementAndGet() == 0) {
                result.completeExceptionally(ex)
            }
        }

        fun send() {
            sendSingleRequest(paymentId, amount, transactionId)
                .thenAcceptAsync({ (success, reason) -> onSuccess(success, reason) }, dbExecutor)
                .exceptionally { ex -> onError(ex.cause ?: ex); null }
        }

        pending.incrementAndGet()
        send()

        val hedgeDelayMs = averageProcessTime.toMillis()
        val isHealthy = circuitBreaker.state == CircuitBreaker.State.CLOSED
        if (isHealthy && remainingMillis(deadline) > hedgeDelayMs * 2) {
            pending.incrementAndGet()
            scheduler.schedule({
                if (!result.isDone) send()
                else pending.decrementAndGet()
            }, hedgeDelayMs, TimeUnit.MILLISECONDS)
        }

        return result
    }

    private fun sendSingleRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
    ): CompletableFuture<Pair<Boolean, String?>> {
        val url = "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .version(Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("x-idempotency-key", transactionId.toString())
            .timeout(Duration.ofSeconds(30))
            .build()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApplyAsync({ response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] Payment response parse error for txId: $transactionId, " +
                                "payment: $paymentId, code: ${response.statusCode()}, body: ${response.body()}"
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                Pair(body.result, body.message)
            }, dbExecutor)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun remainingMillis(epocTime: Long) =
        System.currentTimeMillis().takeIf { it < epocTime }?.let { epocTime - it } ?: 0
}

class PaymentDeclinedException(message: String) : Exception(message)

fun now() = System.currentTimeMillis()