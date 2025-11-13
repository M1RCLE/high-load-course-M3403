package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.time.Duration
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import ru.quipy.common.utils.TokenBucketRateLimiter
import java.util.concurrent.RejectedExecutionException
import kotlin.math.ceil
import kotlin.math.min

@Service
class OrderPayer(
    private val paymentAccountProperties: List<PaymentAccountProperties>,
    registry: MeterRegistry
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val MIN_PARALLEL_PROCESS = 16
        const val MAX_PARALLEL_PROCESS = 256
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    // Расчет ingressRate на основе свойств платежных аккаунтов
    private val ingressRate = calculateIngressRate()

    // Расчет среднего времени обработки
    private val averageProcessingTimeMs = calculateAverageProcessingTime()

    private val paymentExecutor = ThreadPoolExecutor(
        MIN_PARALLEL_PROCESS,
        MAX_PARALLEL_PROCESS,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(256),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val limiter = TokenBucketRateLimiter(
        rate = ingressRate,
        bucketMaxCapacity = ingressRate * 10,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    // Metrics from first example
    private val acceptedCounter: Counter = Counter
        .builder("payments.accepted")
        .register(registry)

    private val rejectedExpired = registry.counter("payments.rejected", "code", "429", "reason", "expired")
    private val rejectedDeadline = registry.counter("payments.rejected", "code", "429", "reason", "deadline_budget")
    private val rejectedLimiter = registry.counter("payments.rejected", "code", "429", "reason", "limiter_throttle")
    private val rejectedQueue = registry.counter("payments.rejected", "code", "429", "reason", "queue_overflow")

    init {
        Gauge.builder("waiting.queue.size") { paymentExecutor.queue.size.toDouble() }
            .description("Tasks waiting in payment submission executor queue")
            .register(registry)

        logger.info("OrderPayer initialized with ingressRate: $ingressRate, averageProcessingTimeMs: $averageProcessingTimeMs, threadPool: ${MIN_PARALLEL_PROCESS}-${MAX_PARALLEL_PROCESS}")
        logger.info("Payment account properties: ${paymentAccountProperties.size} accounts")
        paymentAccountProperties.forEachIndexed { index, props ->
            logger.info("Account $index: rateLimitPerSec=${props.rateLimitPerSec}, averageProcessingTime=${props.averageProcessingTime}")
        }
    }

    private fun calculateIngressRate(): Int {
        return paymentAccountProperties
            .sumOf { it.rateLimitPerSec }
            .coerceAtLeast(1)
            .also { rate ->
                logger.debug("Calculated ingress rate: $rate from ${paymentAccountProperties.size} accounts")
            }
    }

    private fun calculateAverageProcessingTime(): Long {
        return if (paymentAccountProperties.isNotEmpty()) {
            paymentAccountProperties
                .map { it.averageProcessingTime.toMillis() }
                .average()
                .toLong()
                .coerceAtLeast(100) // minimum 100ms
        } else {
            1000L
        }.also { avgTime ->
            logger.debug("Calculated average processing time: ${avgTime}ms")
        }
    }

    private fun now(): Long = System.currentTimeMillis()

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = now()
        val timeBudgetMs = deadline - createdAt

        // Check if request already expired
        if (timeBudgetMs <= 0L) {
            rejectedExpired.increment()
            throw TooManyRequestsException(100)
        }

        // Calculate queue waiting time and check deadline budget
        val qSize = paymentExecutor.queue.size + 1
        val qWaitMs = ((qSize.toDouble() / ingressRate) * 1000).toLong()
        val jitterMs = 300L
        val safety = averageProcessingTimeMs + jitterMs

        if (qWaitMs + safety >= timeBudgetMs) {
            rejectedDeadline.increment()
            val retryBase = ceil(1000.0 / ingressRate).toLong()
            val backoffMs = (retryBase + min(qWaitMs, 2000)).coerceIn(50, 3000)
            throw TooManyRequestsException(backoffMs)
        }

        // Rate limiter check
        if (!limiter.tick()) {
            rejectedLimiter.increment()
            val retryBase = ceil(1000.0 / ingressRate).toLong()
            val currentQWaitMs = ((paymentExecutor.queue.size.toDouble() / ingressRate) * 1000).toLong()
            val backoffMs = (retryBase + min(currentQWaitMs, 2000)).coerceIn(50, 3000)
            throw TooManyRequestsException(backoffMs)
        }

        // Queue capacity check
        if (paymentExecutor.queue.remainingCapacity() == 0) {
            rejectedQueue.increment()
            val backoffMs = (5 * ceil(1000.0 / ingressRate)).toLong()
            throw TooManyRequestsException(backoffMs)
        }

        acceptedCounter.increment()

        val task = Runnable {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        try {
            paymentExecutor.submit(task)
        } catch (ex: RejectedExecutionException) {
            rejectedQueue.increment()
            val qSizeAfterReject = paymentExecutor.queue.size
            val backoffMs = (5 * ceil(1000.0 / ingressRate)).toLong()
            logger.error(
                "paymentExecutor rejected paymentId={}, queueSize={}, activeThreads={}",
                paymentId,
                qSizeAfterReject,
                paymentExecutor.activeCount,
                ex
            )
            throw TooManyRequestsException(backoffMs)
        }

        return createdAt
    }
}

class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many requests")