package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.envelope.PgmqEnvelope
import dev.dotnomi.pgmq.envelope.PgmqHeaderNames
import kotlin.time.Duration

/**
 * Adjusts a message after the declarative defaults but before sending.
 *
 * The escape hatch; prefer the parameter annotations, which are checked at build time. Recognised by
 * type wherever it appears in the parameter list.
 */
fun interface PgmqMessageCustomizer {
    fun customize(builder: PgmqMessageBuilder)

    companion object {
        /** Changes nothing. */
        val NONE: PgmqMessageCustomizer = PgmqMessageCustomizer { }
    }
}

/**
 * Mutable view of a message being built.
 *
 * `messageId`, `sendingTime` and `causationId` are not reachable: overwriting them would break
 * consumer deduplication or sever the causal chain.
 */
class PgmqMessageBuilder internal constructor(
    label: String?,
    targetId: String,
    sourceId: String,
    schemaVersion: Int,
    correlationId: String?,
    private val causationId: String?,
) {
    var label: String? = label
        private set

    var targetId: String = targetId
        private set

    var sourceId: String = sourceId
        private set

    var schemaVersion: Int = schemaVersion
        private set

    /** Settable to deliberately join an existing flow. */
    var correlationId: String? = correlationId
        private set

    var delay: Duration = Duration.ZERO
        private set

    var group: String? = null
        private set

    private val userHeaders: MutableMap<String, String> = LinkedHashMap()

    fun label(value: String?): PgmqMessageBuilder = apply { label = value }

    fun targetId(value: String): PgmqMessageBuilder = apply { targetId = value }

    fun sourceId(value: String): PgmqMessageBuilder = apply { sourceId = value }

    fun schemaVersion(value: Int): PgmqMessageBuilder = apply { schemaVersion = value }

    /** Joins a specific flow instead of inheriting or starting one. */
    fun correlationId(value: String): PgmqMessageBuilder = apply { correlationId = value }

    fun delay(value: Duration): PgmqMessageBuilder = apply { delay = value }

    /** Sets the FIFO group (`x-pgmq-group`). */
    fun group(value: String?): PgmqMessageBuilder = apply { group = value }

    /**
     * Sets a user header.
     *
     * @throws IllegalArgumentException if [name] is reserved
     */
    fun header(name: String, value: String): PgmqMessageBuilder = apply {
        require(!PgmqHeaderNames.isReserved(name)) {
            "Header '$name' is reserved and cannot be set as a user header. Reserved names are " +
                "${PgmqHeaderNames.ENVELOPE_KEYS.sorted()} plus anything under " +
                "'${PgmqHeaderNames.PGMQ_PREFIX}' and 'x-dlq-'."
        }
        userHeaders[name] = value
    }

    fun headers(values: Map<String, String>): PgmqMessageBuilder =
        apply { values.forEach { (name, value) -> header(name, value) } }

    internal fun userHeaders(): Map<String, String> = userHeaders.toMap()

    internal fun buildEnvelope(): PgmqEnvelope = PgmqEnvelope.create(
        sourceId = sourceId,
        targetId = targetId,
        label = label,
        correlationId = correlationId,
        causationId = causationId,
        schemaVersion = schemaVersion,
    )
}
