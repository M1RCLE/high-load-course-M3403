package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
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
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val semaphore = Semaphore(parallelRequests, true)

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
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
        var acquired = semaphoreRequestAcquire(semaphore, deadline)
        // Пытаемся взять блокировку на ограничение параллельных запросов к сервису
        if (!acquired) {
            deadlineHandler(paymentId, transactionId, "Unable to acquire request semaphore")
            return
        }
        try {
            // Если блокировка взята, то пытаемся влезть в окно исполнения до возможного момента вызова
            if (!rateLimiter.blockingUntil(deadline)) {
                deadlineHandler(paymentId, transactionId, "Rate limit exceeded")
                return
            }
            try {

                logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                client.newCall(request).execute().use { response ->
                    semaphore.release().also { acquired = false } // Снимаем семафор пораньше
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        } finally {
            if (acquired) semaphore.release()
        }
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