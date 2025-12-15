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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


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

        const val MAX_RETRIES_AMOUNT = 4
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

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

        performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
            .thenApplyAsync({ result ->
                result
            }, dbExecutor)
            .exceptionally { exception ->
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", exception)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = exception.message ?: "Unknown error")
                }
                false
            }
    }

    private fun performPaymentWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Boolean> {
        if (attempt > MAX_RETRIES_AMOUNT) {
            return CompletableFuture.completedFuture(false)
        }

        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .version(Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(30))
            .build()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenComposeAsync({ response ->
                CompletableFuture.supplyAsync({
                    try {
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        // Убрали warn и info логи для уменьшения overhead в горячем пути

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                        if (body.result) {
                            true
                        } else if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                            // Retry if payment failed
                            performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1).get()
                        } else {
                            false
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
                        if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                            performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1).get()
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                            false
                        }
                    }
                }, dbExecutor)
            }, dbExecutor)
            .exceptionally { exception ->
                val cause = exception.cause
                val isTimeout = cause is HttpTimeoutException
                
                if (isTimeout || cause is IOException) {
                    if (isTimeout) {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt: $attempt", exception)
                    } else {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, attempt: $attempt", exception)
                    }

                    if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                        // Retry on timeout or error
                        performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1).get()
                    } else {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = if (isTimeout) "Request timeout." else exception.message)
                        }
                        false
                    }
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, attempt: $attempt", exception)
                    if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                        performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1).get()
                    } else {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = exception.message)
                        }
                        false
                    }
                }
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