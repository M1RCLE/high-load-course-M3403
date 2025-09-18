package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.rateLimiter.CustomRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val RATE_LIMIT_PERMITS_PER_WINDOW = 20L
    private val RATE_LIMIT_WINDOW = Duration.ofSeconds(1)
    private val MAX_WAIT_DURATION = Duration.ofSeconds(5)

    private val paymentRateLimiter = CustomRateLimiter(
        rate = RATE_LIMIT_PERMITS_PER_WINDOW,
        window = RATE_LIMIT_WINDOW,
    )

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("Attempting to submit payment request for paymentId: $paymentId")

        paymentRateLimiter.tickBlockingDuration(MAX_WAIT_DURATION)

        logger.info("Rate limiter passed, submitting payment request for paymentId: $paymentId")

        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}