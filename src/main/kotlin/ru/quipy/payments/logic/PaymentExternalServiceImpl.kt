package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import kotlin.math.max


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
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Создаем кастомный executor для HTTP клиента
    private val dispatcherExecutor = Executors.newFixedThreadPool(
        max(200, parallelRequests / 2),
        NamedThreadFactory("http-client-$accountName")
    )

    private val dispatcher = Dispatcher(dispatcherExecutor).apply {
        maxRequests = parallelRequests
        maxRequestsPerHost = parallelRequests
    }

    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .connectionPool(ConnectionPool(parallelRequests, 20, java.util.concurrent.TimeUnit.SECONDS))
        .readTimeout(Duration.ofSeconds(30))
        .protocols(listOf(Protocol.HTTP_2))
        .build()

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val cf = CompletableFuture<Boolean>()

        try {
            val windowResult = ongoingWindow.putIntoWindow()
            if (windowResult is NonBlockingOngoingWindow.WindowResponse.Fail) {
                logger.warn("[$accountName] Ongoing window full for payment $paymentId")
                cf.complete(false)
                return cf
            }
            slidingWindowRateLimiter.tickBlocking()

            val urlString = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

            val request = Request.Builder().run {
                url(urlString)
                post(emptyBody)
            }.build()

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    try {
                        if (e is SocketTimeoutException) {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                            } catch (u: Exception) {
                                logger.error("[$accountName] Error while updating ES on timeout for payment $paymentId, txId: $transactionId", u)
                            }
                        } else {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                            } catch (u: Exception) {
                                logger.error("[$accountName] Error while updating ES on failure for payment $paymentId, txId: $transactionId", u)
                            }
                        }
                    } finally {
                        try {
                            ongoingWindow.releaseWindow()
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error releasing ongoingWindow", u)
                        }
                        cf.complete(false)
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        val bodyText = try {
                            response.body?.string()
                        } catch (e: Exception) {
                            null
                        }

                        val body = try {
                            mapper.readValue(bodyText, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                        "payment: $paymentId, result code: ${response.code}, reason: $bodyText",
                                e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message ?: bodyText)
                        }

                        logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message}"
                        )

                        val result = body.result

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error while updating ES on response for payment $paymentId, txId: $transactionId", u)
                        }

                        cf.complete(result)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error while updating ES in exception handler for payment $paymentId, txId: $transactionId", u)
                        } finally {
                            cf.complete(false)
                        }
                    } finally {
                        try {
                            ongoingWindow.releaseWindow()
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error releasing ongoingWindow", u)
                        }
                    }
                }
            })
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
            ongoingWindow.releaseWindow()
            cf.complete(false)
        }

        return cf
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
