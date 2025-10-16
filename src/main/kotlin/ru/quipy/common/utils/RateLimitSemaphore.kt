package ru.quipy.common.utils

import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicInteger

abstract class RateLimitSemaphore<T> protected constructor (private val semaphore: Semaphore) {

    val acquires = AtomicInteger()

    fun acquire(value: T): Boolean {
        return limitAcquire(limit(value))
    }

    fun release() {
        try {
            semaphore.release()
            acquires.decrementAndGet()
        } catch (ex : IllegalStateException) {
            throw ex
        }
    }

    /**
     * Функция вычисления limit для заданного объёма
     */
    protected abstract fun limit(volume: T): Int

    /**
     * Метод берёт семафор, но ограничивая его capacity в пределах limit, который меньше, чем даёт сам семафор
     */
    private fun limitAcquire(limit: Int): Boolean {
        // Берём семафор
        if (limit > 0 && semaphore.tryAcquire()) {
            // Если успешно получили, то оцениваем количество захватов
            if (acquires.incrementAndGet() <= limit) {
                // Если не превысили лимит, то возвращаем, что всё ОК
                return true
            }
            release()
        }
        return false
    }

}