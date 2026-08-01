package dev.dotnomi.pgmq.envelope

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * Translates between a [PgmqEnvelope] plus user headers and pgmq's `headers` jsonb.
 *
 * Both sit flat in the same object, so `headers->>'label'` works in psql.
 */
class EnvelopeCodec(private val mapper: ObjectMapper) {

    /** Builds the `headers` JSON. Rejects user headers using a reserved name. */
    fun encode(
        envelope: PgmqEnvelope?,
        userHeaders: Map<String, String> = emptyMap(),
        group: String? = null,
    ): String {
        val node = mapper.createObjectNode()

        if (envelope != null) {
            node.put(PgmqHeaderNames.MESSAGE_ID, envelope.messageId)
            node.put(PgmqHeaderNames.SOURCE_ID, envelope.sourceId)
            node.put(PgmqHeaderNames.TARGET_ID, envelope.targetId)
            if (envelope.label != null) node.put(PgmqHeaderNames.LABEL, envelope.label)
            node.put(PgmqHeaderNames.CORRELATION_ID, envelope.correlationId)
            if (envelope.causationId != null) node.put(PgmqHeaderNames.CAUSATION_ID, envelope.causationId)
            node.put(PgmqHeaderNames.SCHEMA_VERSION, envelope.schemaVersion)
            node.put(
                PgmqHeaderNames.SENDING_TIME,
                PgmqEnvelope.SENDING_TIME_FORMAT.format(envelope.sendingTime),
            )
        }

        if (group != null) node.put(PgmqHeaderNames.GROUP, group)

        userHeaders.forEach { (key, value) ->
            require(!PgmqHeaderNames.isReserved(key)) {
                "Header '$key' is reserved and cannot be set as a user header. Reserved are " +
                    "${PgmqHeaderNames.ENVELOPE_KEYS.sorted()} plus anything under " +
                    "'${PgmqHeaderNames.PGMQ_PREFIX}' and 'x-dlq-'."
            }
            node.put(key, value)
        }

        return mapper.writeValueAsString(node)
    }

    /** Like [encode], but also writes the reserved `x-dlq-*` diagnostics. */
    fun encodeForDeadLetter(
        envelope: PgmqEnvelope?,
        userHeaders: Map<String, String>,
        group: String?,
        diagnostics: Map<String, String>,
    ): String {
        val base = mapper.readTree(encode(envelope, userHeaders, group)) as ObjectNode
        diagnostics.forEach { (key, value) -> base.put(key, value) }
        return mapper.writeValueAsString(base)
    }

    /** Reads the envelope, or `null` when the message has none. */
    fun decode(headersJson: String?): PgmqEnvelope? = try {
        decodeOrThrow(headersJson)
    } catch (_: PgmqEnvelopeException) {
        null
    }

    /** Like [decode], but throws with a reason that ends up as `x-dlq-reason`. */
    fun decodeOrThrow(headersJson: String?): PgmqEnvelope {
        if (headersJson.isNullOrBlank()) {
            throw PgmqEnvelopeException("Message has no headers, so there is no envelope.")
        }

        val node: JsonNode = try {
            mapper.readTree(headersJson)
        } catch (e: Exception) {
            throw PgmqEnvelopeException("headers is not valid JSON: ${e.message}", e)
        }

        if (node !is ObjectNode) {
            throw PgmqEnvelopeException("headers is not a JSON object but ${node.nodeType}.")
        }

        val missing = listOf(
            PgmqHeaderNames.MESSAGE_ID,
            PgmqHeaderNames.SOURCE_ID,
            PgmqHeaderNames.TARGET_ID,
            PgmqHeaderNames.CORRELATION_ID,
            PgmqHeaderNames.SENDING_TIME,
        ).filter { node.get(it)?.isNull != false }

        if (missing.isNotEmpty()) {
            throw PgmqEnvelopeException("Envelope is incomplete, missing fields: $missing")
        }

        val sendingTimeRaw = node.get(PgmqHeaderNames.SENDING_TIME).asText()
        val sendingTime = try {
            Instant.parse(sendingTimeRaw)
        } catch (e: DateTimeParseException) {
            throw PgmqEnvelopeException(
                "sendingTime '$sendingTimeRaw' is not an ISO-8601 timestamp.", e,
            )
        }

        return PgmqEnvelope(
            messageId = node.get(PgmqHeaderNames.MESSAGE_ID).asText(),
            sourceId = node.get(PgmqHeaderNames.SOURCE_ID).asText(),
            targetId = node.get(PgmqHeaderNames.TARGET_ID).asText(),
            label = node.get(PgmqHeaderNames.LABEL)?.takeIf { !it.isNull }?.asText(),
            correlationId = node.get(PgmqHeaderNames.CORRELATION_ID).asText(),
            causationId = node.get(PgmqHeaderNames.CAUSATION_ID)?.takeIf { !it.isNull }?.asText(),
            schemaVersion = node.get(PgmqHeaderNames.SCHEMA_VERSION)
                ?.takeIf { !it.isNull }?.asInt(PgmqEnvelope.DEFAULT_SCHEMA_VERSION)
                ?: PgmqEnvelope.DEFAULT_SCHEMA_VERSION,
            sendingTime = sendingTime,
        )
    }

    /** Everything except the reserved names. */
    fun decodeUserHeaders(headersJson: String?): Map<String, String> {
        if (headersJson.isNullOrBlank()) return emptyMap()
        val node = runCatching { mapper.readTree(headersJson) }.getOrNull() ?: return emptyMap()
        if (node !is ObjectNode) return emptyMap()

        return buildMap {
            node.fieldNames().forEach { name ->
                if (!PgmqHeaderNames.isReserved(name)) {
                    node.get(name)?.let { put(name, if (it.isTextual) it.asText() else it.toString()) }
                }
            }
        }
    }

    /** The `x-dlq-*` diagnostics, which are reserved and so absent from the user headers. */
    fun decodeDiagnostics(headersJson: String?): Map<String, String> {
        if (headersJson.isNullOrBlank()) return emptyMap()
        val node = runCatching { mapper.readTree(headersJson) }.getOrNull() ?: return emptyMap()
        if (node !is ObjectNode) return emptyMap()

        return buildMap {
            node.fieldNames().forEach { name ->
                if (name.startsWith("x-dlq-")) {
                    node.get(name)?.let { put(name, if (it.isTextual) it.asText() else it.toString()) }
                }
            }
        }
    }

    /** The FIFO group, when set. */
    fun decodeGroup(headersJson: String?): String? {
        if (headersJson.isNullOrBlank()) return null
        val node = runCatching { mapper.readTree(headersJson) }.getOrNull() ?: return null
        return node.get(PgmqHeaderNames.GROUP)?.takeIf { !it.isNull }?.asText()
    }
}
