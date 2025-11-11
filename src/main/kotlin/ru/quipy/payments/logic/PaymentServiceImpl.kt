package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.*


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
                val startRequestTime = System.currentTimeMillis()

                val (res, statusCode) = account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)

                val requestDuration = System.currentTimeMillis() - startRequestTime

                DistributionSummary.builder("request_latency")
                    .description("Request latency.")
                    .tag("status_code", statusCode.toString())
                    .publishPercentiles(0.5, 0.8, 0.99)
                    .register(meterRegistry)
                    .record(requestDuration.toDouble())

                if (res) {
                    break
                } else {
                    Thread.sleep((10 * i).toLong())
                }
            }
        }
    }
}