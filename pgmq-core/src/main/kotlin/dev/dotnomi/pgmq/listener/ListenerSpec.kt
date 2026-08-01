package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.PgmqMessage
import dev.dotnomi.pgmq.envelope.EnvelopeValidation
import dev.dotnomi.pgmq.serializer.PgmqType
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** How a successfully processed message is acknowledged. */
enum class AckMode {
    /** Delete it for good. The normal case. */
    DELETE,

    /** Move it to the archive table so it stays auditable. */
    ARCHIVE,

    /** The handler acknowledges itself via [PgmqContext.ack] or [PgmqContext.archive]. */
    MANUAL,
}

/** What happens to a message no handler claims. */
enum class UnroutableMessagePolicy {
    /** Dead letter it. Default, since an unknown label is almost always a deployment mistake. */
    DLQ,
    ARCHIVE,
    IGNORE,
}

/** Which read function the container uses. */
enum class ReadStrategy {
    /** `pgmq.read` — no ordering guarantee, full concurrency. */
    PLAIN,

    /** `pgmq.read_grouped` — ordering within a group. Safe with `concurrency > 1`. */
    GROUPED,

    /** `pgmq.read_grouped_rr` — spreads one call across groups. Requires `concurrency = 1`. */
    GROUPED_ROUND_ROBIN,
}

/**
 * Configuration of a listener container.
 *
 * Applies per queue, not per handler: all handlers of a queue share one container.
 */
data class ListenerSpec(
    val queue: String,
    val client: String = DEFAULT_CLIENT,

    /** Parallel worker threads. */
    val concurrency: Int = 1,

    /**
     * Messages fetched per `read`.
     *
     * A larger batch trades redelivery latency for fewer read round-trips: on an ungraceful kill up
     * to `batchSize - 1` messages wait for the visibility timeout to expire.
     */
    val batchSize: Int = 1,

    /** Wait time only when the queue was empty; otherwise reading continues at once. */
    val pollInterval: Duration = 5.seconds,

    /** Minimum spacing between processing starts, shared across the container's workers. */
    val messageInterval: Duration = Duration.ZERO,

    /** `null` derives it from [batchSize] and [messageInterval]. */
    val visibilityTimeout: Duration? = null,

    /** Extends the visibility timeout during processing so a batch cannot outlive it. */
    val vtRefresh: Boolean = false,

    val ackMode: AckMode = AckMode.DELETE,

    /** After this many failed deliveries the message is dead lettered. */
    val maxRetries: Int = 5,

    /** `null` derives the name from the queue; see [effectiveDeadLetterQueue]. */
    val deadLetterQueue: String? = null,

    val autoCreateQueue: Boolean = true,

    /** Whether the container starts consuming on its own. */
    val autoStart: Boolean = true,

    val readStrategy: ReadStrategy = ReadStrategy.PLAIN,

    val envelopeValidation: EnvelopeValidation = EnvelopeValidation.STRICT,

    val unroutable: UnroutableMessagePolicy = UnroutableMessagePolicy.DLQ,

    /** Upper bound for the exponential backoff between retries. */
    val maxRetryBackoff: Duration = 5.minutes,

    /** Additional exception class names treated as permanent (no retry). */
    val nonRetryableExceptions: Set<String> = emptySet(),

    /** How long shutdown waits before abandoning an in-flight message. */
    val shutdownTimeout: Duration = 30.seconds,
) {
    init {
        require(concurrency >= 1) { "concurrency must be at least 1 but was $concurrency." }
        require(batchSize >= 1) { "batchSize must be at least 1 but was $batchSize." }
        require(maxRetries >= 0) { "maxRetries must not be negative but was $maxRetries." }
        require(!messageInterval.isNegative()) { "messageInterval must not be negative." }

        if (readStrategy == ReadStrategy.GROUPED_ROUND_ROBIN) {
            require(concurrency == 1) {
                "readStrategy GROUPED_ROUND_ROBIN spreads one read across several groups and " +
                    "leaves nothing for additional workers, so concurrency must be 1 but was " +
                    "$concurrency. Use GROUPED for concurrency with ordering guarantees."
            }
        }

        if (visibilityTimeout != null && messageInterval > Duration.ZERO) {
            val batchDuration = messageInterval * batchSize
            require(visibilityTimeout > batchDuration) {
                "visibilityTimeout ($visibilityTimeout) is too short: with batchSize=$batchSize " +
                    "and messageInterval=$messageInterval a batch needs at least $batchDuration, so " +
                    "the last messages would become visible again and be processed twice. Raise " +
                    "visibilityTimeout, lower batchSize, or enable vtRefresh."
            }
        }
    }

    /** The value actually used — derived when not set explicitly. */
    val effectiveVisibilityTimeout: Duration
        get() = visibilityTimeout ?: run {
            val batchDuration = messageInterval * batchSize
            maxOf(DEFAULT_VISIBILITY_TIMEOUT, batchDuration * VISIBILITY_SAFETY_FACTOR)
        }

    /** Derived as `<queue>_dlq`, hashed down when that would exceed pgmq's 47-character limit. */
    val effectiveDeadLetterQueue: String
        get() = deadLetterQueue ?: deriveDeadLetterQueue(queue)

    /** `<queue>`, or `<client>/<queue>` for a named client. */
    val id: String
        get() = if (client == DEFAULT_CLIENT) queue else "$client/$queue"

    companion object {
        const val DEFAULT_CLIENT: String = "<default>"
        val DEFAULT_VISIBILITY_TIMEOUT: Duration = 30.seconds

        /** Headroom on top of the raw batch duration, so processing time itself still fits. */
        const val VISIBILITY_SAFETY_FACTOR: Int = 3

        /** pgmq rejects anything longer. */
        const val MAX_QUEUE_NAME_LENGTH: Int = 47

        private const val DLQ_SUFFIX = "_dlq"
        private const val DLQ_HASH_LENGTH = 6

        internal fun deriveDeadLetterQueue(queue: String): String {
            val candidate = queue + DLQ_SUFFIX
            if (candidate.length <= MAX_QUEUE_NAME_LENGTH) return candidate

            val hash = java.security.MessageDigest.getInstance("SHA-256")
                .digest(queue.toByteArray())
                .joinToString("") { "%02x".format(it) }
                .take(DLQ_HASH_LENGTH)
            val room = MAX_QUEUE_NAME_LENGTH - DLQ_SUFFIX.length - DLQ_HASH_LENGTH - 1
            return "${queue.take(room)}_$hash$DLQ_SUFFIX"
        }
    }
}

/** What a handler can do with the message it is processing. */
interface PgmqContext {
    val queue: String
    val message: PgmqMessage<*>

    /** Acknowledges the message by deleting it. Only needed with [AckMode.MANUAL]. */
    fun ack()

    /** Acknowledges the message by archiving it. Only needed with [AckMode.MANUAL]. */
    fun archive()

    /** Redelivers after [delay] without counting as a failure. */
    fun retryAfter(delay: Duration)
}

/**
 * A handler registered within a container.
 *
 * The payload type is applied after label dispatch, so each message is parsed once into the right type.
 */
class RegisteredHandler<T>(
    /** `null` makes this the catch-all of its queue. */
    val label: String?,
    val payloadType: PgmqType<T>,
    /** Whether the handler receives the whole batch at once. */
    val batch: Boolean = false,
    /**
     * Skips deserialization and hands the payload over as the raw JSON text.
     *
     * [payloadType] must then be `String`.
     */
    val raw: Boolean = false,
    /** Supported payload format versions; `null` accepts any. */
    val schemaVersions: IntRange? = null,
    /** Name shown in logs and listings. */
    val name: String = label ?: "catch-all",
    private val invoke: (List<PgmqMessage<T>>, PgmqContext) -> Unit,
) {
    internal fun handle(messages: List<PgmqMessage<T>>, context: PgmqContext) = invoke(messages, context)

    fun supports(schemaVersion: Int): Boolean = schemaVersions?.contains(schemaVersion) ?: true
}

/** Marks an exception class as permanent: dead lettered on the first attempt, without retry. */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqNonRetryable

/** Base class for permanent failures; alternative to [PgmqNonRetryable]. */
open class PgmqPermanentException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)
