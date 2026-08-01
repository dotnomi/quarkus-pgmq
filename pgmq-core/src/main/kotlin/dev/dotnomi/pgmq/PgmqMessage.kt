package dev.dotnomi.pgmq

import dev.dotnomi.pgmq.envelope.PgmqEnvelope
import java.time.Instant

/**
 * A message read from a queue, with a typed payload.
 *
 * Carries two identities: [msgId] is pgmq's row id, unique only within its queue; the envelope's
 * `messageId` is the logical identity, stable across rows, queues and replays.
 */
data class PgmqMessage<T>(
    val msgId: Long,
    val readCount: Int,
    val enqueuedAt: Instant,
    val lastReadAt: Instant?,
    val visibleAt: Instant,
    val payload: T,
    /** User headers. Envelope and diagnostic fields are not included. */
    val headers: Map<String, String>,
    /** `null` when the message was published without an envelope. */
    val envelope: PgmqEnvelope?,
    /** FIFO group from `x-pgmq-group`. */
    val group: String?,
    /** `x-dlq-*` headers when this message sits in a dead letter queue. Empty otherwise. */
    val diagnostics: Map<String, String> = emptyMap(),
) {
    /** Shorthand for the envelope's label. */
    val label: String? get() = envelope?.label

    /** How often the message has been delivered. */
    val deliveryAttempt: Int get() = readCount

    fun <R> withPayload(newPayload: R): PgmqMessage<R> = PgmqMessage(
        msgId = msgId,
        readCount = readCount,
        enqueuedAt = enqueuedAt,
        lastReadAt = lastReadAt,
        visibleAt = visibleAt,
        payload = newPayload,
        headers = headers,
        envelope = envelope,
        group = group,
        diagnostics = diagnostics,
    )
}

/** Queue metadata as returned by `pgmq.list_queues()`. */
data class QueueInfo(
    val name: String,
    val isPartitioned: Boolean,
    val isUnlogged: Boolean,
    val createdAt: Instant,
)

/**
 * Queue metrics from `pgmq.metrics()`.
 *
 * [visibleLength] counts only messages that can be picked up now, excluding those in flight — the
 * value to alert a backlog on.
 */
data class QueueMetrics(
    val name: String,
    val length: Long,
    val visibleLength: Long,
    val newestMessageAgeSeconds: Int?,
    val oldestMessageAgeSeconds: Int?,
    val totalMessages: Long,
    val scrapedAt: Instant,
)
