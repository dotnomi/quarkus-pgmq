package dev.dotnomi.pgmq

import dev.dotnomi.pgmq.envelope.PgmqEnvelope

/**
 * Holds the envelope of the message being processed, for the duration of one handler call.
 *
 * This is what makes a `send` inside a handler inherit the incoming `correlationId` and set
 * `causationId` automatically.
 */
object PgmqExchangeContext {

    private val current = ThreadLocal<PgmqEnvelope?>()

    /** The envelope being processed, or `null` outside a handler. */
    fun current(): PgmqEnvelope? = current.get()

    /** Runs [block] with [envelope] as the current context, restoring the previous value afterwards. */
    fun <R> with(envelope: PgmqEnvelope?, block: () -> R): R {
        val previous = current.get()
        current.set(envelope)
        return try {
            block()
        } finally {
            if (previous == null) current.remove() else current.set(previous)
        }
    }

    /** The correlation fields a message sent right now should inherit. Empty outside a handler. */
    fun inherited(): InheritedCorrelation {
        val parent = current.get() ?: return InheritedCorrelation(null, null)
        return InheritedCorrelation(correlationId = parent.correlationId, causationId = parent.messageId)
    }

    data class InheritedCorrelation(val correlationId: String?, val causationId: String?)
}

/** Whether a publisher continues the current flow or starts a new one. */
enum class CorrelationPolicy {
    /** Inherit `correlationId` from the context when present. */
    INHERIT,

    /** Always start a fresh flow, for entry points such as schedulers. */
    NEW,
}
