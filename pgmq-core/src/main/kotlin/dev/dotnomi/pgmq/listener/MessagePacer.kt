package dev.dotnomi.pgmq.listener

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration

/**
 * Enforces a minimum spacing between processing starts.
 *
 * Shared across a container's workers, so `concurrency` does not multiply the configured rate.
 */
internal class MessagePacer(private val interval: Duration) {

    private val lock = ReentrantLock()
    private var nextSlotNanos: Long = Long.MIN_VALUE

    val enabled: Boolean get() = interval > Duration.ZERO

    /** Reserves the next slot and waits for it. Returns early only when the thread is interrupted. */
    fun await() {
        if (!enabled) return

        val slotNanos = lock.withLock {
            val now = System.nanoTime()
            val slot = if (nextSlotNanos == Long.MIN_VALUE) now else maxOf(now, nextSlotNanos)
            nextSlotNanos = slot + interval.inWholeNanoseconds
            slot
        }

        // parkNanos may return early, so loop until the reserved instant is reached.
        while (!Thread.currentThread().isInterrupted) {
            val remaining = slotNanos - System.nanoTime()
            if (remaining <= 0) break
            LockSupport.parkNanos(remaining)
        }
    }
}
