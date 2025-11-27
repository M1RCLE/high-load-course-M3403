package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
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

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_RETRIES_AMOUNT = 4
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Создаем кастомный executor для HTTP клиента
    private val httpClientExecutor = Executors.newFixedThreadPool(
        parallelRequests.coerceAtLeast(16),
        NamedThreadFactory("http-client-$accountName")
    )
    
    // Создаем executor для обработки ответов
    private val responseHandlingExecutor = Executors.newFixedThreadPool(
        16,
        NamedThreadFactory("response-handler-$accountName")
    )

    private val dispatcher = Dispatcher(httpClientExecutor).apply {
        maxRequests = parallelRequests * 2
        maxRequestsPerHost = parallelRequests * 2
    }

    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .build()

    private val semaphore = Semaphore(parallelRequests, true)

    private val rateLimiter = LeakingBucketRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
        (rateLimitPerSec * 1.2).toInt() // Example bucket size: use something reasonable or make configurable
    )

    fun deadlineHandler(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline by reason: $reason")
        }
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId. Reason: $reason")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Try to submit payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        
        // Пытаемся взять блокировку на ограничение параллельных запросов к сервису
        val acquired = semaphoreRequestAcquire(semaphore, deadline)
        if (!acquired) {
            deadlineHandler(paymentId, transactionId, "Unable to acquire request semaphore")
            return
        }
        
        // Если блокировка взята, то пытаемся влезть в окно исполнения до возможного момента вызова
        if (!rateLimiter.tick()) {
            semaphore.release()
            deadlineHandler(paymentId, transactionId, "Rate limit exceeded")
            return
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Используем асинхронный вызов с CompletableFuture
        performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
            .thenApplyAsync({ result ->
                semaphore.release()
                result
            }, responseHandlingExecutor)
            .exceptionally { exception ->
                semaphore.release()
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

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        val future = CompletableFuture<Boolean>()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                responseHandlingExecutor.submit {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                            if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                                // Retry on timeout
                                performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                                    .thenAccept { result -> future.complete(result) }
                            } else {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                                future.complete(false)
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                            if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                                // Retry on error
                                performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                                    .thenAccept { result -> future.complete(result) }
                            } else {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                                future.complete(false)
                            }
                        }
                    }
                }
            }

            override fun onResponse(call: Call, response: Response) {
                responseHandlingExecutor.submit {
                    try {
                        response.use {
                            val body = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            logger.info("[$accountName] Payment passed with result: ${body.result}, and message: ${body.message}, attempt number: $attempt")

                            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            if (body.result) {
                                future.complete(true)
                            } else if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                                // Retry if payment failed
                                performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                                    .thenAccept { result -> future.complete(result) }
                            } else {
                                future.complete(false)
                            }
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
                        if (attempt < MAX_RETRIES_AMOUNT && remainingMillis(deadline) > averageProcessTime.toMillis()) {
                            performPaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                                .thenAccept { result -> future.complete(result) }
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                            future.complete(false)
                        }
                    }
                }
            }
        })

        return future
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    /**
     * Пробуем взять семафор, но не позднее момента протухания запроса (чуть ранее)
     */
    private fun semaphoreRequestAcquire(semaphore: Semaphore, epocTime: Long) =
        remainingRequestMillis(epocTime).takeIf { it > 0 } // Если ещё есть время на блокировку
            ?.let { semaphore.tryAcquire(it, TimeUnit.MILLISECONDS) }
            ?: false

    /**
     * Сколько миллисекунд осталось до завершения запрос с учётом средней возможной задержки
     */
    private fun remainingRequestMillis(epocTime: Long) =
        remainingMillis(epocTime)// - averageProcessTime.toMillis() - 75)

    /**
     * Сколько миллисекунд осталось до заданного момента
     */
    private fun remainingMillis(epocTime: Long) =
        System.currentTimeMillis().takeIf { it < epocTime }
            ?.let { epocTime - it }
            ?: 0

}

fun now() = System.currentTimeMillis()