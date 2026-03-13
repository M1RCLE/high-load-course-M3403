package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.InstantRateLimitSemaphore
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.TooManyRequestsException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.roundToInt
import kotlin.math.roundToLong

@Service
class OrderPayer(paymentAccountProperties: List<PaymentAccountProperties>) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val MIN_PARALLEL_PROCESS = 64
        const val MAX_PARALLEL_PROCESS = 512
        const val DELAY_COEFFICIENT = 0.0
        const val MIN_DELAY_ADD_MILLIS = 10L
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val parallelThreads = paymentAccountProperties
        .sumOf { it.rateLimitPerSec.coerceAtMost(MAX_PARALLEL_PROCESS) }
    private val poolSize = (parallelThreads + 2)
        .coerceAtLeast(MIN_PARALLEL_PROCESS)
        .coerceAtMost(MAX_PARALLEL_PROCESS)

    // Увеличена очередь до 50000 для обработки большего количества запросов
    private val paymentExecutor = ThreadPoolExecutor(
        poolSize,
        poolSize,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(50_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val callsPerMinute = paymentAccountProperties.sumOf {
        (TimeUnit.MINUTES.toNanos(1).toDouble() *
                it.rateLimitPerSec.coerceAtMost(MAX_PARALLEL_PROCESS) /
                it.averageProcessingTime.toNanos()).roundToInt()}

    private val minAverageProcessingTime = paymentAccountProperties.minOf { it.averageProcessingTime }
    private val maxAverageProcessingTime = paymentAccountProperties.maxOf { it.averageProcessingTime }


    /**
     * Это предполагаемое время, которое может понадобиться внешнему сервису на выполнение нашего запроса
     * Т.е.: если у нас до deathTime остаётся меньше callDelay, то мы его не ставим в очередь, а получаем
     * отказ от семафора
     */
    val callDelay: Duration = Duration
        .ofMillis((minAverageProcessingTime.toMillis() * DELAY_COEFFICIENT)
            .roundToLong()
            .coerceAtLeast(MIN_DELAY_ADD_MILLIS))

    val instantRateLimitSemaphore =
        Triple(
            // Делаем объем по задачам на несколько секунды вперед.
            // То есть если есть свободные места в очереди на эти 3 величины обработки запроса и задаче не протухнет
            // до того момента, когда сможет выполниться, то мы её ставим в очередь, а если нет,
            // то возвращаем TooManyRequests
            maxAverageProcessingTime.multipliedBy(3),
            TimeUnit.MINUTES,
            callsPerMinute
        ).let {
            logger.info("Create OrderPayer::InstantRateLimitSemaphore(duration=${it.first}, timeUnit=${it.second}, rate=${it.third})")
            InstantRateLimitSemaphore(it.first, it.second, it.third)
        }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val deadLineTime = Instant.ofEpochMilli(deadline)
        if (instantRateLimitSemaphore.acquire(deadLineTime.minus(callDelay))) {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }
                    logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                } finally {
                    instantRateLimitSemaphore.release()
                }
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } else {
            logger.error("Payment: $paymentId retried. Too many requests")
            throw TooManyRequestsException()
        }
        return createdAt
    }
}
