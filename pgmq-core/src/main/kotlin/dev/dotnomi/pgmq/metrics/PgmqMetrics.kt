package dev.dotnomi.pgmq.metrics

import kotlin.time.Duration

/** Why a message ended up in the dead letter queue. Kept coarse so it is safe as a metric tag. */
enum class DeadLetterReason {
    /** The payload could not be deserialized, or the envelope failed validation. */
    MALFORMED,

    /** No handler claimed the label and the queue has no catch-all. */
    UNROUTABLE,

    /** The payload format version is outside what the handler declared it supports. */
    SCHEMA_VERSION,

    /** The handler failed with an error classified as permanent, so retrying was pointless. */
    PERMANENT_FAILURE,

    /** The handler kept failing until `maxRetries` ran out. */
    RETRIES_EXHAUSTED,
}

/** How processing a single message ended. */
enum class MessageOutcome {
    SUCCESS,

    /** Failed, but will be delivered again after the backoff. */
    RETRIED,

    DEAD_LETTERED,

    /** No handler claimed the message; what happened to it depends on the unroutable policy. */
    UNROUTABLE,
}

/**
 * Observation hook for everything a listener or a publisher does.
 *
 * The core deliberately has no metrics library of its own, so this stays a plain interface with a
 * no-op default; `quarkus-pgmq` supplies a Micrometer-backed implementation when metrics are on.
 *
 * Implementations are called on the worker threads and must not block — anything slow here directly
 * slows down message processing.
 *
 * **Tags come from here, so only bounded values may be passed.** `label` must be the label of the
 * *handler* that ran, never the label of the incoming message: a freely chosen label would create a
 * new time series per distinct value and eventually take down the metrics backend.
 */
interface PgmqMetrics {

    fun messageProcessed(queue: String, label: String, outcome: MessageOutcome, duration: Duration) {}

    fun messageDeadLettered(queue: String, reason: DeadLetterReason) {}

    fun messagePublished(queue: String) {}

    companion object {
        val NOOP: PgmqMetrics = object : PgmqMetrics {}
    }
}
