package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpClient.Version
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean


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

        const val HEDGE_DELAY_FRACTION = 0.7
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val hedgeDelayMs = (averageProcessTime.toMillis() * HEDGE_DELAY_FRACTION).toLong()

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

    private val hedgeScheduler: ScheduledExecutorService =
        Executors.newScheduledThreadPool(2, NamedThreadFactory("hedge-scheduler"))

    private val client = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .executor(httpClientExecutor)
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        (rateLimitPerSec * 0.95).toLong(),
        Duration.ofSeconds(1)
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        rateLimiter.tickBlocking()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        performHedgedPayment(paymentId, amount, transactionId, deadline)
            .thenApplyAsync({ result -> result }, dbExecutor)
            .exceptionally { exception ->
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", exception)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = exception.message ?: "Unknown error")
                }
                false
            }
    }


    private fun performHedgedPayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): CompletableFuture<Boolean> {
        val result = CompletableFuture<Boolean>()
        val settled = AtomicBoolean(false)

        fun handleResponse(future: CompletableFuture<ExternalSysResponse>) {
            future.thenAcceptAsync({ body ->
                if (settled.compareAndSet(false, true)) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    result.complete(body.result)
                }
                // else: the other hedged call already settled the result — discard this response
            }, dbExecutor).exceptionally { ex ->
                if (settled.compareAndSet(false, true)) {
                    result.completeExceptionally(ex)
                }
                null
            }
        }

        // First request
        handleResponse(sendPaymentRequest(paymentId, amount, transactionId, deadline))

        // Schedule hedge request after hedgeDelayMs
        val hedgeTask = hedgeScheduler.schedule({
            if (!result.isDone && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                logger.info("[$accountName] Hedged request triggered for txId: $transactionId, payment: $paymentId")
                handleResponse(sendPaymentRequest(paymentId, amount, transactionId, deadline))
            }
        }, hedgeDelayMs, TimeUnit.MILLISECONDS)

        // Cancel the pending hedge task once we already have an answer
        result.whenComplete { _, _ -> hedgeTask.cancel(false) }

        return result
    }


    private fun sendPaymentRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): CompletableFuture<ExternalSysResponse> {
        val timeoutMs = remainingMillis(deadline).coerceAtMost(30_000L).coerceAtLeast(1L)
        val url = "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .version(Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofMillis(timeoutMs))
            .build()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApplyAsync({ response ->
                try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] [ERROR] Failed to parse response for txId: $transactionId, " +
                                "payment: $paymentId, status: ${response.statusCode()}, body: ${response.body()}"
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }, httpClientExecutor)
            .exceptionally { ex ->
                val cause = ex.cause ?: ex
                when (cause) {
                    is HttpTimeoutException ->
                        logger.error("[$accountName] Timeout for txId: $transactionId, payment: $paymentId")
                    is IOException ->
                        logger.error("[$accountName] IO error for txId: $transactionId, payment: $paymentId", cause)
                    else ->
                        logger.error("[$accountName] Error for txId: $transactionId, payment: $paymentId", cause)
                }
                throw cause
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    /**
     * Сколько миллисекунд осталось до заданного момента
     */
    private fun remainingMillis(epocTime: Long) =
        System.currentTimeMillis().takeIf { it < epocTime }
            ?.let { epocTime - it }
            ?: 0

}

fun now() = System.currentTimeMillis()
