package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.rateLimiter.CustomRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val RATE_LIMIT_PERMITS_PER_WINDOW = 20L
    private val RATE_LIMIT_WINDOW = Duration.ofSeconds(5)
    private val MAX_WAIT_DURATION = Duration.ofSeconds(1)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentRateLimiter = CustomRateLimiter(
        rate = RATE_LIMIT_PERMITS_PER_WINDOW,
        window = RATE_LIMIT_WINDOW,
    )

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): ProcessPaymentResult {
        val createdAt = System.currentTimeMillis()

        val rateLimitPermitAcquired = paymentRateLimiter.tickBlockingDuration(MAX_WAIT_DURATION)

        if (!rateLimitPermitAcquired) {
            logger.warn("Rate limit exceeded for payment $paymentId, order $orderId. Request rejected after ${MAX_WAIT_DURATION.toMillis()}ms wait.")
            return ProcessPaymentResult(createdAt, false, "Rate limit exceeded")
        }

        try {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                    logger.debug("Payment request submitted for payment $paymentId, order $orderId")
                } catch (e: Exception) {
                    logger.error("Error processing payment $paymentId for order $orderId", e)
                    throw e
                }
            }

            logger.debug("Payment $paymentId for order $orderId queued for processing")
            return ProcessPaymentResult(createdAt, true, null)

        } catch (e: Exception) {
            logger.error("Failed to submit payment $paymentId for order $orderId to executor", e)
            return ProcessPaymentResult(createdAt, false, "Failed to queue payment: ${e.message}")
        }
    }

    data class ProcessPaymentResult(
        val createdAt: Long,
        val success: Boolean,
        val errorMessage: String?
    )
}