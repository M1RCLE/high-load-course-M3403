package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val httpClientExecutor = ThreadPoolExecutor(
        64,
        64,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(100000),
        NamedThreadFactory("payment-http-client")
    )

    private val dbExecutor = ThreadPoolExecutor(
        1000,
        1000,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(50000),
        NamedThreadFactory("payment-db-callback")
    )

    private val scheduler = Executors.newScheduledThreadPool(8, NamedThreadFactory("payment-hedge-scheduler"))

    private val client = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .executor(httpClientExecutor)
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        rateLimiter.tickBlocking()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        performHedgedPayment(paymentId, amount, transactionId, deadline)
            .exceptionally { exception ->
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", exception)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = exception.message ?: "Unknown error")
                }
                false
            }
    }

    /**
     * Отправляет первый запрос и если за какое-то вермя  не получен ответ,
     *  * то отправляет параллельный запрос с тем же transactionId в качестве
     * ключа идемпотентности. Короче побеждает тот кто ответил первым
     */
    private fun performHedgedPayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): CompletableFuture<Boolean> {
        val result = CompletableFuture<Boolean>()
        // сколько запросов у нас щас. когда будет 0, то значит что все запросы отправили уже и они вернулись с ответами(дай бог)
        val pending = AtomicInteger(0)

        fun onSuccess(success: Boolean, reason: String?) {
            // result.complete() атомарно вернёт true только первому вызову
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
            // Таймаут HTTP привязываем к дедлайну — не ждём 30с если платёж уже не успеет
            val timeoutMs = remainingMillis(deadline).coerceAtLeast(500L)
            sendSingleRequest(paymentId, amount, transactionId, timeoutMs)
                .thenAcceptAsync({ (success, reason) -> onSuccess(success, reason) }, dbExecutor)
                .exceptionally { ex -> onError(ex.cause ?: ex); null }
        }

        pending.incrementAndGet()
        send()

        fun scheduleHedge(delayMs: Long) {
            if (remainingMillis(deadline) > delayMs) {
                pending.incrementAndGet()
                scheduler.schedule({
                    if (!result.isDone) send()
                    else pending.decrementAndGet()
                }, delayMs, TimeUnit.MILLISECONDS)
            }
        }

        val avg = averageProcessTime.toMillis()
        // 5 попыток равномерно по шкале дедлайна
        scheduleHedge((avg * 0.10).toLong().coerceAtLeast(150L))  // ~1300ms остаток
        scheduleHedge((avg * 0.22).toLong().coerceAtLeast(280L))  // ~1170ms остаток
        scheduleHedge((avg * 0.38).toLong().coerceAtLeast(450L))  // ~1000ms остаток
        scheduleHedge((avg * 0.55).toLong().coerceAtLeast(650L))  // ~800ms  остаток

        return result
    }

    private fun sendSingleRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        timeoutMs: Long = 30_000L,
    ): CompletableFuture<Pair<Boolean, String?>> {
        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .version(Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("x-idempotency-key", transactionId.toString())
            .timeout(Duration.ofMillis(timeoutMs))
            .build()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApplyAsync({ response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment response parse error for txId: $transactionId, payment: $paymentId, code: ${response.statusCode()}, body: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                Pair(body.result, body.message)
            }, dbExecutor)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun remainingMillis(epocTime: Long) =
        System.currentTimeMillis().takeIf { it < epocTime }
            ?.let { epocTime - it }
            ?: 0

}

fun now() = System.currentTimeMillis()