package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.metrics.DeadLetterReason
import dev.dotnomi.pgmq.metrics.MessageOutcome
import dev.dotnomi.pgmq.metrics.PgmqMetrics
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Singleton
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Reports the core's observations to Micrometer, and through it to Prometheus.
 *
 * Registered only when `pgmq.metrics.enabled=true` and a Micrometer registry exists — otherwise the
 * core keeps its no-op and costs nothing.
 *
 * Meters produced:
 *
 * | Meter | Type | Tags |
 * |---|---|---|
 * | `pgmq.message.processing` | Timer | `queue`, `label`, `outcome` |
 * | `pgmq.messages.dead.lettered` | Counter | `queue`, `reason` |
 * | `pgmq.messages.published` | Counter | `queue` |
 *
 * The timer carries the throughput as its own count, so no separate counter for processed messages
 * exists: `rate(pgmq_message_processing_seconds_count[5m])` is the message rate, and the quantiles
 * of the same meter are the handler latency.
 */
@Singleton
class PgmqMicrometerMetrics(private val registry: MeterRegistry) : PgmqMetrics {

    override fun messageProcessed(
        queue: String,
        label: String,
        outcome: MessageOutcome,
        duration: Duration,
    ) {
        registry.timer(
            "pgmq.message.processing",
            "queue", queue,
            "label", label,
            "outcome", outcome.name.lowercase(),
        ).record(duration.toJavaDuration())
    }

    override fun messageDeadLettered(queue: String, reason: DeadLetterReason) {
        registry.counter(
            "pgmq.messages.dead.lettered",
            "queue", queue,
            "reason", reason.name.lowercase(),
        ).increment()
    }

    override fun messagePublished(queue: String) {
        registry.counter("pgmq.messages.published", "queue", queue).increment()
    }
}
