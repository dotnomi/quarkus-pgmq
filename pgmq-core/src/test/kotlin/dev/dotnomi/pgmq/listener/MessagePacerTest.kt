package dev.dotnomi.pgmq.listener

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.nanoseconds

/**
 * The pacing guarantee, tested where it holds exactly.
 *
 * A container test can only take its timestamps inside the handler, with deserialization and handler
 * lookup in between — that jitter is real and makes an exact assertion there impossible. Here there
 * is nothing between the reserved slot and the measurement, so the guarantee can be pinned without
 * tolerance.
 */
class MessagePacerTest {

    private inline fun elapsed(block: () -> Unit): Duration {
        val start = System.nanoTime()
        block()
        return (System.nanoTime() - start).nanoseconds
    }

    @Test
    fun `n calls take at least n-1 intervals`() {
        val pacer = MessagePacer(100.milliseconds)

        // The first call reserves "now" and returns immediately; each further one waits a full slot.
        val took = elapsed { repeat(5) { pacer.await() } }

        assertThat(took).isGreaterThanOrEqualTo(400.milliseconds)
    }

    @Test
    fun `a zero interval disables the pacer entirely`() {
        val pacer = MessagePacer(Duration.ZERO)
        assertThat(pacer.enabled).isFalse()

        val took = elapsed { repeat(1000) { pacer.await() } }

        // The only upper bound in this class, so the only assertion that gets *more* likely to fail
        // the slower the machine is. One second is far above the microseconds this actually takes,
        // and still orders of magnitude below the 100+ seconds any real interval would cost.
        assertThat(took).isLessThan(1.seconds)
    }

    @Test
    fun `slow work does not build up a debt that is repaid as a burst`() {
        val pacer = MessagePacer(50.milliseconds)

        pacer.await()
        Thread.sleep(300)

        // Five intervals' worth of time has passed. A pacer that tracked "owed" slots would now let
        // five messages through at once, which is exactly the burst the throttle exists to prevent.
        val took = elapsed { repeat(3) { pacer.await() } }

        assertThat(took)
            .describedAs("the first of the three may start at once, the other two must still wait")
            .isGreaterThanOrEqualTo(100.milliseconds)
    }

    @Test
    fun `the rate is shared across threads rather than applying per thread`() {
        val pacer = MessagePacer(100.milliseconds)
        val callsPerThread = 3
        val threads = 4
        val ready = CountDownLatch(threads)
        val go = CountDownLatch(1)

        val start: Long
        val workers = (1..threads).map {
            thread {
                ready.countDown()
                go.await()
                repeat(callsPerThread) { pacer.await() }
            }
        }
        ready.await(5, TimeUnit.SECONDS)
        start = System.nanoTime()
        go.countDown()
        workers.forEach { it.join() }
        val took = (System.nanoTime() - start) / 1_000_000

        // 12 slots across all threads, so 11 intervals — not 2 intervals as it would be if every
        // thread paced itself. Getting this wrong multiplies the configured rate by the concurrency.
        assertThat(took)
            .describedAs("%d threads x %d calls at 100ms must take ~1.1s in total", threads, callsPerThread)
            .isGreaterThanOrEqualTo((threads * callsPerThread - 1) * 100L)
    }
}
