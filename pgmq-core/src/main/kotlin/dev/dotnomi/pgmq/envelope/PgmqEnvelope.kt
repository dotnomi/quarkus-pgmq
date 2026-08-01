package dev.dotnomi.pgmq.envelope

import dev.dotnomi.pgmq.UuidV7
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * The header contract for every message published through this library.
 *
 * Stored flat in pgmq's `headers` jsonb. [messageId], [sendingTime] and [causationId] are managed by
 * the library and cannot be set from outside.
 */
data class PgmqEnvelope(
    /**
     * Logical identity (UUIDv7), stable across retries, dead letter replays and topic fan-out.
     * The idempotency key.
     */
    val messageId: String,

    /** Originating system or tenant. */
    val sourceId: String,

    /** Target system. An assertion for the consumer to check, not a routing mechanism. */
    val targetId: String,

    /** Optional message type. Routes between several handlers on one queue. */
    val label: String?,

    /** Flow id, inherited by messages published from inside a handler. */
    val correlationId: String,

    /** `messageId` of the causing message; `null` at the start of a chain. */
    val causationId: String?,

    /** Payload format version. */
    val schemaVersion: Int,

    /** Time of sending, UTC. */
    val sendingTime: Instant,
) {
    companion object {
        const val DEFAULT_SCHEMA_VERSION: Int = 1

        /** Fixed three fractional digits; `Instant.toString()` would drop a zero fraction. */
        internal val SENDING_TIME_FORMAT: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

        /** Creates an envelope for a message about to be sent. */
        fun create(
            sourceId: String,
            targetId: String,
            label: String? = null,
            correlationId: String? = null,
            causationId: String? = null,
            schemaVersion: Int = DEFAULT_SCHEMA_VERSION,
        ): PgmqEnvelope = PgmqEnvelope(
            messageId = UuidV7.generateString(),
            sourceId = sourceId,
            targetId = targetId,
            label = label,
            correlationId = correlationId ?: UuidV7.generateString(),
            causationId = causationId,
            schemaVersion = schemaVersion,
            sendingTime = Instant.now(),
        )
    }
}

/** Reserved header names. A user header occupying one of them is rejected. */
object PgmqHeaderNames {
    const val MESSAGE_ID: String = "messageId"
    const val SOURCE_ID: String = "sourceId"
    const val TARGET_ID: String = "targetId"
    const val LABEL: String = "label"
    const val CORRELATION_ID: String = "correlationId"
    const val CAUSATION_ID: String = "causationId"
    const val SCHEMA_VERSION: String = "schemaVersion"
    const val SENDING_TIME: String = "sendingTime"

    /** Set by pgmq: the FIFO group used by `read_grouped`. */
    const val GROUP: String = "x-pgmq-group"

    /** Prefix reserved for internal headers. */
    const val PGMQ_PREFIX: String = "x-pgmq-"

    /** Added when a message is dead lettered. */
    const val DLQ_REASON: String = "x-dlq-reason"
    const val DLQ_ORIGIN_QUEUE: String = "x-dlq-origin-queue"
    const val DLQ_ORIGIN_MSG_ID: String = "x-dlq-origin-msg-id"
    const val DLQ_READ_CT: String = "x-dlq-read-ct"

    val ENVELOPE_KEYS: Set<String> = setOf(
        MESSAGE_ID, SOURCE_ID, TARGET_ID, LABEL,
        CORRELATION_ID, CAUSATION_ID, SCHEMA_VERSION, SENDING_TIME,
    )

    fun isReserved(key: String): Boolean =
        key in ENVELOPE_KEYS || key.startsWith(PGMQ_PREFIX) || key.startsWith("x-dlq-")
}

/** How strictly a consumer validates an incoming envelope. */
enum class EnvelopeValidation {
    /** Missing or incomplete envelope is dead lettered without retry. */
    STRICT,

    /** Missing fields are defaulted; the message is processed. */
    LENIENT,

    /** The envelope is not evaluated. For queues fed by foreign clients. */
    OFF,
}

/** Permanent failure: the envelope is unusable, so a retry cannot help. */
class PgmqEnvelopeException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)
