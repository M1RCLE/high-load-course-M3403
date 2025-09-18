package ru.quipy.common.utils.rateLimiter

import java.time.Duration

interface RateLimitable {
    fun tick(): Boolean
    fun tickBlockingDuration(duration: Duration): Boolean
}