package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.PgmqMessage
import dev.dotnomi.pgmq.serializer.pgmqType

/** Builds a handler for a single message. `label = null` makes it the catch-all of its queue. */
inline fun <reified T> pgmqHandler(
    label: String? = null,
    schemaVersions: IntRange? = null,
    name: String = label ?: "catch-all",
    crossinline block: (PgmqMessage<T>) -> Unit,
): RegisteredHandler<T> = RegisteredHandler(
    label = label,
    payloadType = pgmqType<T>(),
    batch = false,
    schemaVersions = schemaVersions,
    name = name,
    invoke = { messages, _ -> messages.forEach { block(it) } },
)

/** Like [pgmqHandler], with the [PgmqContext] passed to the handler. */
inline fun <reified T> pgmqHandlerWithContext(
    label: String? = null,
    schemaVersions: IntRange? = null,
    name: String = label ?: "catch-all",
    crossinline block: (PgmqMessage<T>, PgmqContext) -> Unit,
): RegisteredHandler<T> = RegisteredHandler(
    label = label,
    payloadType = pgmqType<T>(),
    batch = false,
    schemaVersions = schemaVersions,
    name = name,
    invoke = { messages, context -> messages.forEach { block(it, context) } },
)

/** Builds a handler that only receives the payload. */
inline fun <reified T> pgmqPayloadHandler(
    label: String? = null,
    schemaVersions: IntRange? = null,
    name: String = label ?: "catch-all",
    crossinline block: (T) -> Unit,
): RegisteredHandler<T> = pgmqHandler(label, schemaVersions, name) { block(it.payload) }

/** Builds a handler that receives the whole batch in one call. */
inline fun <reified T> pgmqBatchHandler(
    label: String? = null,
    schemaVersions: IntRange? = null,
    name: String = label ?: "batch",
    crossinline block: (List<PgmqMessage<T>>) -> Unit,
): RegisteredHandler<T> = RegisteredHandler(
    label = label,
    payloadType = pgmqType<T>(),
    batch = true,
    schemaVersions = schemaVersions,
    name = name,
    invoke = { messages, _ -> block(messages) },
)

/**
 * Builds a handler that receives the payload as the raw JSON text, without deserializing it.
 *
 * The escape hatch when the shape is unknown, varies, or the mapping should stay in your own code:
 * a `String` handler only works when the body itself is a JSON string, whereas this receives any
 * shape verbatim.
 */
inline fun pgmqRawHandler(
    label: String? = null,
    name: String = label ?: "raw",
    crossinline block: (String) -> Unit,
): RegisteredHandler<String> = RegisteredHandler(
    label = label,
    payloadType = pgmqType<String>(),
    batch = false,
    raw = true,
    name = name,
    invoke = { messages, _ -> messages.forEach { block(it.payload) } },
)

/** Like [pgmqRawHandler], with the full message available. */
inline fun pgmqRawMessageHandler(
    label: String? = null,
    name: String = label ?: "raw",
    crossinline block: (PgmqMessage<String>) -> Unit,
): RegisteredHandler<String> = RegisteredHandler(
    label = label,
    payloadType = pgmqType<String>(),
    batch = false,
    raw = true,
    name = name,
    invoke = { messages, _ -> messages.forEach { block(it) } },
)
