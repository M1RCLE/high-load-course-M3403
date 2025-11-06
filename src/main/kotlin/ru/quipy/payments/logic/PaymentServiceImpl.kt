package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    @Autowired val meterRegistry: MeterRegistry,
) : PaymentService {
    private val ansCounter: Counter = Counter.builder("payment_service_sanded")
        .description("Total number of sanded requests")
        .tag("service", "payment_requests")
        .register(meterRegistry)

    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)

        const val MAX_RETRIES = 4
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            for (i in 1..MAX_RETRIES) {
                val res = account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                if (res) {
                    break
                } else {
                    Thread.sleep((10 * i).toLong())
                }
            }
        }
    }
}