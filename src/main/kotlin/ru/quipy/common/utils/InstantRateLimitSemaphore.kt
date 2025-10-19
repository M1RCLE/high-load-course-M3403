package ru.quipy.common.utils

import kotlinx.coroutines.sync.Semaphore
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.floor

/**
 * Обёртка над семафором, которая учитывает сколько семафоров можно взять в единицу времени и если до указанного
 * момента осталось меньше, чем limitDuration единиц времени, то ограничивает возможность взятия семафора
 */
class InstantRateLimitSemaphore(
    limitDuration: Duration,
    private val rateDuration: Duration,
    private val unitsPerRate: Int
): RateLimitSemaphore<Instant>(Semaphore(durationCapacity(limitDuration, rateDuration, unitsPerRate)) ) {

    constructor(limitDuration: Duration, timeUnit: TimeUnit, unitsPerRate: Int) :
            this(limitDuration, Duration.ofNanos(timeUnit.toNanos(1)), unitsPerRate)

    /**
     * Функция вычисляет допустимый limit взятий семафора на заданный период
     */
    override fun limit(volume: Instant): Int =
        Duration.between(Instant.now(), volume)
            ?.takeIf { it.toNanos() > 0 }
            ?.let { durationCapacity(it, rateDuration, unitsPerRate) }
            ?: 0

    companion object {
        /**
         * Вычисление количества ресурса на Duration при известном объёме на единицу TimeUnit
         * @param duration Длительность
         * @param rateDuration Длительность периода
         * @param unitsPerRate Количество ресурса на период
         * @return количество элементов, доступных на duration
         */
        private fun durationCapacity(
            duration: Duration,
            rateDuration: Duration,
            unitsPerRate: Int) =
            floor(unitsPerRate.toDouble() * duration.toNanos() / rateDuration.toNanos()).toInt()
    }

}
