package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(Measure(1, System.currentTimeMillis()))
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null) {
                delay(1L)
                continue
            }
            if (head.timestamp > winStart) {
                delay(head.timestamp - winStart)
                continue
            }
            sum.addAndGet(-1)
            queue.take()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}

class Semaphore(permits: Int) {
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private var availablePermits = permits

    @Throws(InterruptedException::class)
    fun acquire() {
        lock.withLock {
            while (availablePermits <= 0) {
                condition.await()
            }
            availablePermits--
        }
    }

    fun tryAcquire(): Boolean {
        return lock.withLock {
            if (availablePermits > 0) {
                availablePermits--
                true
            } else {
                false
            }
        }
    }

    // работает не чини, так что потом переписать можно будет на промежуточную acquire
    @Throws(InterruptedException::class)
    fun tryAcquire(timeout: Long, unit: TimeUnit): Boolean {
        var remainingNanos = unit.toNanos(timeout)
        lock.lock()
        try {
            while (availablePermits <= 0) {
                if (remainingNanos <= 0) {
                    return false
                }
                remainingNanos = condition.awaitNanos(remainingNanos)
            }
            availablePermits--
            return true
        } finally {
            lock.unlock()
        }
    }

    fun release() {
        lock.withLock {
            availablePermits++
            condition.signal()
        }
    }

    fun <T> permitTask (task: () -> T): T {
        acquire()
        try {
            return task()
        } finally {
            release()
        }
    }

}
