package dev.dotnomi.pgmq

import dev.dotnomi.pgmq.envelope.EnvelopeCodec
import dev.dotnomi.pgmq.metrics.PgmqMetrics
import dev.dotnomi.pgmq.envelope.PgmqEnvelope
import dev.dotnomi.pgmq.internal.ConnectionSource
import dev.dotnomi.pgmq.internal.DataSourceConnectionSource
import dev.dotnomi.pgmq.internal.FixedConnectionSource
import dev.dotnomi.pgmq.internal.Identifiers
import dev.dotnomi.pgmq.permissions.PgmqPermissionOperations
import dev.dotnomi.pgmq.topics.PgmqTopicOperations
import dev.dotnomi.pgmq.serializer.JacksonPgmqSerializer
import dev.dotnomi.pgmq.serializer.PgmqSerializer
import dev.dotnomi.pgmq.serializer.PgmqType
import dev.dotnomi.pgmq.serializer.pgmqType
import java.sql.Connection
import java.sql.ResultSet
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Entry point for every queue operation: publishing, reading, administration and metrics.
 *
 * Built from a [DataSource], so any pool works. Thread-safe as long as the datasource is; the
 * derivations from [withConnection] and [inTransaction] are bound to one connection and are not.
 */
class PgmqTemplate private constructor(
    private val connections: ConnectionSource,
    val serializer: PgmqSerializer,
    private val codec: EnvelopeCodec,
    private val defaultSourceId: String,
    private val metrics: PgmqMetrics,
) {
    @JvmOverloads
    constructor(
        dataSource: DataSource,
        serializer: PgmqSerializer = JacksonPgmqSerializer(),
        sourceId: String = DEFAULT_SOURCE_ID,
        metrics: PgmqMetrics = PgmqMetrics.NOOP,
    ) : this(
        connections = DataSourceConnectionSource(dataSource),
        serializer = serializer,
        codec = EnvelopeCodec(
            (serializer as? JacksonPgmqSerializer)?.mapper ?: JacksonPgmqSerializer.defaultMapper(),
        ),
        defaultSourceId = sourceId,
        metrics = metrics,
    )

    // --- Transactions ---

    /** Binds to an existing connection, e.g. to publish inside the caller's transaction. */
    fun withConnection(connection: Connection): PgmqTemplate =
        PgmqTemplate(FixedConnectionSource(connection), serializer, codec, defaultSourceId, metrics)

    /**
     * Runs [block] in one transaction.
     *
     * Not for FIFO reads: `read_grouped` holds a queue-wide lock until the transaction ends, which
     * would starve every other worker.
     */
    fun <R> inTransaction(block: (PgmqTemplate) -> R): R = connections.use { conn ->
        val previousAutoCommit = conn.autoCommit
        conn.autoCommit = false
        try {
            val result = block(withConnection(conn))
            conn.commit()
            result
        } catch (e: Throwable) {
            runCatching { conn.rollback() }
            throw e
        } finally {
            runCatching { conn.autoCommit = previousAutoCommit }
        }
    }

    // --- Queue administration ---

    fun createQueue(queue: String): Unit = callVoid("SELECT pgmq.create(?)", queue)

    fun createUnloggedQueue(queue: String): Unit =
        callVoid("SELECT pgmq.create_unlogged(?)", queue)

    fun createPartitionedQueue(
        queue: String,
        partitionInterval: String = "10000",
        retentionInterval: String = "100000",
    ) {
        connections.use { conn ->
            conn.prepareStatement("SELECT pgmq.create_partitioned(?, ?, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setString(2, partitionInterval)
                ps.setString(3, retentionInterval)
                ps.execute()
            }
        }
    }

    /** Creates the queue unless it exists. Idempotent. */
    fun createQueueIfMissing(queue: String) {
        if (!queueExists(queue)) {
            createQueue(queue)
        }
    }

    fun dropQueue(queue: String): Boolean = connections.use { conn ->
        conn.prepareStatement("SELECT pgmq.drop_queue(?)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
        }
    }

    fun purgeQueue(queue: String): Long = connections.use { conn ->
        conn.prepareStatement("SELECT pgmq.purge_queue(?)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else 0L }
        }
    }

    fun listQueues(): List<QueueInfo> = connections.use { conn ->
        conn.prepareStatement("SELECT queue_name, is_partitioned, is_unlogged, created_at FROM pgmq.list_queues()")
            .use { ps ->
                ps.executeQuery().use { rs ->
                    buildList {
                        while (rs.next()) {
                            add(
                                QueueInfo(
                                    name = rs.getString("queue_name"),
                                    isPartitioned = rs.getBoolean("is_partitioned"),
                                    isUnlogged = rs.getBoolean("is_unlogged"),
                                    createdAt = rs.instant("created_at")!!,
                                ),
                            )
                        }
                    }
                }
            }
    }

    fun queueExists(queue: String): Boolean = connections.use { conn ->
        conn.prepareStatement("SELECT EXISTS(SELECT 1 FROM pgmq.meta WHERE queue_name = ?)").use { ps ->
            ps.setString(1, queue)
            ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
        }
    }

    fun metrics(queue: String): QueueMetrics = connections.use { conn ->
        conn.prepareStatement("SELECT * FROM pgmq.metrics(?)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.executeQuery().use { rs ->
                require(rs.next()) { "Queue '$queue' does not exist." }
                rs.toQueueMetrics()
            }
        }
    }

    fun metricsAll(): List<QueueMetrics> = connections.use { conn ->
        conn.prepareStatement("SELECT * FROM pgmq.metrics_all()").use { ps ->
            ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.toQueueMetrics()) } }
        }
    }

    /** Index for FIFO grouping; without it `read_grouped` gets slow on larger queues. */
    fun createFifoIndex(queue: String): Unit =
        callVoid("SELECT pgmq.create_fifo_index(?)", queue)

    /**
     * Enables `NOTIFY` on insert.
     *
     * The listeners here poll instead, to keep control of the processing rate. Exposed for foreign
     * consumers that want push.
     */
    fun enableNotifyInsert(queue: String, throttle: Duration = 250.milliseconds) {
        connections.use { conn ->
            conn.prepareStatement("SELECT pgmq.enable_notify_insert(?, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setInt(2, throttle.inWholeMilliseconds.toInt())
                ps.execute()
            }
        }
    }

    fun disableNotifyInsert(queue: String): Unit =
        callVoid("SELECT pgmq.disable_notify_insert(?)", queue)

    /** Per-queue permission management: which role may publish, which may consume. */
    fun permissions(): PgmqPermissionOperations = PgmqPermissionOperations(connections)

    /**
     * Topics: one publish reaches every subscriber.
     *
     * Call [PgmqTopicOperations.initialiseSchema] once at startup.
     */
    fun topics(): PgmqTopicOperations = PgmqTopicOperations(this)

    /** Runs [block] on a connection from this template's source. */
    internal fun <R> runOnConnection(block: (Connection) -> R): R = connections.use(block)

    /** The installed pgmq version, or `null` when the extension is absent. */
    fun extensionVersion(): String? = connections.use { conn ->
        conn.prepareStatement("SELECT extversion FROM pg_extension WHERE extname = 'pgmq'").use { ps ->
            ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
        }
    }

    // --- Publishing ---

    /**
     * Publishes a message.
     *
     * `messageId` and `sendingTime` are generated here; the correlation fields are inherited from
     * [PgmqExchangeContext] when called inside a handler.
     */
    @JvmOverloads
    fun send(
        queue: String,
        payload: Any?,
        label: String? = null,
        targetId: String = DEFAULT_TARGET_ID,
        sourceId: String = defaultSourceId,
        headers: Map<String, String> = emptyMap(),
        group: String? = null,
        schemaVersion: Int = PgmqEnvelope.DEFAULT_SCHEMA_VERSION,
        correlation: CorrelationPolicy = CorrelationPolicy.INHERIT,
        delay: Duration = Duration.ZERO,
    ): Long {
        val envelope = newEnvelope(label, targetId, sourceId, schemaVersion, correlation)
        return sendEnvelope(queue, payload, envelope, headers, group, delay)
    }

    /** Like [send], with an absolute visibility time instead of a delay. */
    fun sendAt(
        queue: String,
        payload: Any?,
        visibleAt: Instant,
        label: String? = null,
        targetId: String = DEFAULT_TARGET_ID,
        sourceId: String = defaultSourceId,
        headers: Map<String, String> = emptyMap(),
        group: String? = null,
        schemaVersion: Int = PgmqEnvelope.DEFAULT_SCHEMA_VERSION,
        correlation: CorrelationPolicy = CorrelationPolicy.INHERIT,
    ): Long {
        val envelope = newEnvelope(label, targetId, sourceId, schemaVersion, correlation)
        return connections.use { conn ->
            // The cast is required: pgmq.send is overloaded on integer and timestamptz.
            conn.prepareStatement("SELECT pgmq.send(?, ?::jsonb, ?::jsonb, ?::timestamptz)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setString(2, serializer.serialize(payload))
                ps.setString(3, codec.encode(envelope, headers, group))
                // OffsetDateTime rather than Timestamp, so the JVM time zone does not matter.
                ps.setObject(4, visibleAt.atOffset(ZoneOffset.UTC))
                ps.executeQuery().use { rs ->
                    require(rs.next()) { "pgmq.send returned no msg_id." }
                    metrics.messagePublished(queue)
                    rs.getLong(1)
                }
            }
        }
    }

    /** Publishes with a pre-built envelope. */
    fun sendEnvelope(
        queue: String,
        payload: Any?,
        envelope: PgmqEnvelope,
        headers: Map<String, String> = emptyMap(),
        group: String? = null,
        delay: Duration = Duration.ZERO,
    ): Long = connections.use { conn ->
        // Explicit `::integer` because of the timestamptz overload.
        conn.prepareStatement("SELECT pgmq.send(?, ?::jsonb, ?::jsonb, ?::integer)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.setString(2, serializer.serialize(payload))
            ps.setString(3, codec.encode(envelope, headers, group))
            ps.setInt(4, delay.inWholeSeconds.toInt())
            ps.executeQuery().use { rs ->
                require(rs.next()) { "pgmq.send returned no msg_id." }
                metrics.messagePublished(queue)
                rs.getLong(1)
            }
        }
    }

    /**
     * Publishes several messages in one statement.
     *
     * The arrays are passed as a single JSON array and split server-side, which avoids the escaping
     * pitfalls of JDBC arrays with a jsonb element type.
     */
    fun sendBatch(
        queue: String,
        payloads: List<Any?>,
        label: String? = null,
        targetId: String = DEFAULT_TARGET_ID,
        sourceId: String = defaultSourceId,
        headers: Map<String, String> = emptyMap(),
        group: String? = null,
        schemaVersion: Int = PgmqEnvelope.DEFAULT_SCHEMA_VERSION,
        correlation: CorrelationPolicy = CorrelationPolicy.INHERIT,
        delay: Duration = Duration.ZERO,
    ): List<Long> {
        if (payloads.isEmpty()) return emptyList()

        // One envelope per message: a shared messageId would defeat consumer deduplication.
        val envelopeJsons = payloads.map {
            codec.encode(newEnvelope(label, targetId, sourceId, schemaVersion, correlation), headers, group)
        }
        val payloadArray = "[${payloads.joinToString(",") { serializer.serialize(it) }}]"
        val headerArray = "[${envelopeJsons.joinToString(",")}]"

        return connections.use { conn ->
            conn.prepareStatement(
                """
                SELECT pgmq.send_batch(
                    ?,
                    ARRAY(SELECT jsonb_array_elements(?::jsonb)),
                    ARRAY(SELECT jsonb_array_elements(?::jsonb)),
                    ?::integer
                )
                """.trimIndent(),
            ).use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setString(2, payloadArray)
                ps.setString(3, headerArray)
                ps.setInt(4, delay.inWholeSeconds.toInt())
                ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) } }
                    .also { ids -> repeat(ids.size) { metrics.messagePublished(queue) } }
            }
        }
    }

    private fun newEnvelope(
        label: String?,
        targetId: String,
        sourceId: String,
        schemaVersion: Int,
        correlation: CorrelationPolicy,
    ): PgmqEnvelope {
        val inherited = when (correlation) {
            CorrelationPolicy.INHERIT -> PgmqExchangeContext.inherited()
            CorrelationPolicy.NEW -> PgmqExchangeContext.InheritedCorrelation(null, null)
        }
        return PgmqEnvelope.create(
            sourceId = sourceId,
            targetId = targetId,
            label = label,
            correlationId = inherited.correlationId,
            causationId = inherited.causationId,
            schemaVersion = schemaVersion,
        )
    }

    // --- Reading ---

    fun <T> read(
        queue: String,
        type: PgmqType<T>,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
        conditional: String? = null,
    ): List<PgmqMessage<T>> = readVia(
        sql = "SELECT * FROM pgmq.read(?, ?, ?, ?::jsonb)",
        queue = queue,
        type = type,
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
        extraParams = listOf(conditional ?: "{}"),
    )

    /** Long-polling variant. Not used by the listeners, which poll to keep rate control. */
    fun <T> readWithPoll(
        queue: String,
        type: PgmqType<T>,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
        maxPoll: Duration = 5.seconds,
        pollInterval: Duration = 100.milliseconds,
        conditional: String? = null,
    ): List<PgmqMessage<T>> = readVia(
        sql = "SELECT * FROM pgmq.read_with_poll(?, ?, ?, ?, ?, ?::jsonb)",
        queue = queue,
        type = type,
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
        extraParams = listOf(
            maxPoll.inWholeSeconds.toInt(),
            pollInterval.inWholeMilliseconds.toInt(),
            conditional ?: "{}",
        ),
    )

    /**
     * Reads messages of one FIFO group (`x-pgmq-group`).
     *
     * Groups with unacknowledged messages are skipped, so ordering holds across workers. Holds a
     * queue-wide lock until the transaction ends, so it must run in its own short transaction.
     */
    fun <T> readGrouped(
        queue: String,
        type: PgmqType<T>,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
    ): List<PgmqMessage<T>> = readVia(
        sql = "SELECT * FROM pgmq.read_grouped(?, ?, ?)",
        queue = queue,
        type = type,
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
    )

    /**
     * Round-robin across groups, so a hot group cannot starve the others.
     *
     * One call spans several groups and leaves nothing for other workers, so use `concurrency = 1`.
     */
    fun <T> readGroupedRoundRobin(
        queue: String,
        type: PgmqType<T>,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
    ): List<PgmqMessage<T>> = readVia(
        sql = "SELECT * FROM pgmq.read_grouped_rr(?, ?, ?)",
        queue = queue,
        type = type,
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
    )

    /** Reads and deletes in one step — no visibility timeout, so no redelivery. */
    fun <T> pop(queue: String, type: PgmqType<T>, quantity: Int = 1): List<PgmqMessage<T>> =
        connections.use { conn ->
            conn.prepareStatement("SELECT * FROM pgmq.pop(?, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setInt(2, quantity)
                ps.executeQuery().use { rs -> rs.toMessages { serializer.deserialize(it, type) } }
            }
        }

    /**
     * Like [read], but leaves the payload as raw JSON.
     *
     * Used by the listener container, where the target type is only known after label dispatch, and
     * by tools that move messages without knowing their type.
     */
    @JvmOverloads
    fun readRaw(
        queue: String,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
        conditional: String? = null,
    ): List<PgmqMessage<String>> = readVia(
        sql = "SELECT * FROM pgmq.read(?, ?, ?, ?::jsonb)",
        queue = queue,
        mapPayload = { it },
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
        extraParams = listOf(conditional ?: "{}"),
    )

    /** FIFO variant of [readRaw]. */
    fun readGroupedRaw(
        queue: String,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
    ): List<PgmqMessage<String>> = readVia(
        sql = "SELECT * FROM pgmq.read_grouped(?, ?, ?)",
        queue = queue,
        mapPayload = { it },
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
    )

    /** Round-robin variant of [readRaw]. */
    fun readGroupedRoundRobinRaw(
        queue: String,
        visibilityTimeout: Duration = DEFAULT_VISIBILITY_TIMEOUT,
        quantity: Int = 1,
    ): List<PgmqMessage<String>> = readVia(
        sql = "SELECT * FROM pgmq.read_grouped_rr(?, ?, ?)",
        queue = queue,
        mapPayload = { it },
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
    )

    /** Deserializes a raw payload into the target type. */
    fun <T> convertPayload(rawJson: String, type: PgmqType<T>): T =
        serializer.deserialize(rawJson, type)

    /**
     * Moves a message unchanged into another queue and adds diagnostics. The path into the DLQ.
     *
     * The payload is passed through raw — it may be here precisely because it would not deserialize
     * — and the envelope is kept, so a replay stays deduplicable.
     */
    fun sendToDeadLetter(
        deadLetterQueue: String,
        source: PgmqMessage<String>,
        diagnostics: Map<String, String>,
    ): Long = connections.use { conn ->
        val headersJson = codec.encodeForDeadLetter(
            envelope = source.envelope,
            userHeaders = source.headers,
            group = source.group,
            diagnostics = diagnostics,
        )
        conn.prepareStatement("SELECT pgmq.send(?, ?::jsonb, ?::jsonb, ?::integer)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(deadLetterQueue))
            ps.setString(2, source.payload)
            ps.setString(3, headersJson)
            ps.setInt(4, 0)
            // Deliberately not counted as published: a dead letter is reported through
            // messageDeadLettered, and counting it here too would inflate the publish rate.
            ps.executeQuery().use { rs ->
                require(rs.next()) { "pgmq.send returned no msg_id." }
                rs.getLong(1)
            }
        }
    }

    private fun <T> readVia(
        sql: String,
        queue: String,
        type: PgmqType<T>,
        visibilityTimeout: Duration,
        quantity: Int,
        extraParams: List<Any> = emptyList(),
    ): List<PgmqMessage<T>> = readVia(
        sql = sql,
        queue = queue,
        mapPayload = { serializer.deserialize(it, type) },
        visibilityTimeout = visibilityTimeout,
        quantity = quantity,
        extraParams = extraParams,
    )

    private fun <T> readVia(
        sql: String,
        queue: String,
        mapPayload: (String) -> T,
        visibilityTimeout: Duration,
        quantity: Int,
        extraParams: List<Any> = emptyList(),
    ): List<PgmqMessage<T>> = connections.use { conn ->
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.setInt(2, visibilityTimeout.inWholeSeconds.toInt())
            ps.setInt(3, quantity)
            extraParams.forEachIndexed { index, value ->
                when (value) {
                    is Int -> ps.setInt(index + 4, value)
                    is String -> ps.setString(index + 4, value)
                    else -> ps.setObject(index + 4, value)
                }
            }
            ps.executeQuery().use { rs -> rs.toMessages(mapPayload) }
        }
    }

    // --- Acknowledgement and visibility ---

    fun delete(queue: String, msgId: Long): Boolean = connections.use { conn ->
        conn.prepareStatement("SELECT pgmq.delete(?, ?::bigint)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.setLong(2, msgId)
            ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
        }
    }

    fun delete(queue: String, msgIds: List<Long>): List<Long> {
        if (msgIds.isEmpty()) return emptyList()
        return connections.use { conn ->
            conn.prepareStatement("SELECT pgmq.delete(?, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setArray(2, conn.createArrayOf("bigint", msgIds.toTypedArray()))
                ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) } }
            }
        }
    }

    /** Moves the message to the archive table instead of deleting it. */
    fun archive(queue: String, msgId: Long): Boolean = connections.use { conn ->
        conn.prepareStatement("SELECT pgmq.archive(?, ?::bigint)").use { ps ->
            ps.setString(1, Identifiers.requireValidQueueName(queue))
            ps.setLong(2, msgId)
            ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
        }
    }

    fun archive(queue: String, msgIds: List<Long>): List<Long> {
        if (msgIds.isEmpty()) return emptyList()
        return connections.use { conn ->
            conn.prepareStatement("SELECT pgmq.archive(?, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setArray(2, conn.createArrayOf("bigint", msgIds.toTypedArray()))
                ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) } }
            }
        }
    }

    /** Sets the visibility timeout. [Duration.ZERO] makes the message visible immediately. */
    fun setVisibilityTimeout(queue: String, msgId: Long, timeout: Duration) {
        connections.use { conn ->
            conn.prepareStatement("SELECT msg_id FROM pgmq.set_vt(?, ?::bigint, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setLong(2, msgId)
                ps.setInt(3, timeout.inWholeSeconds.toInt())
                ps.executeQuery().use { /* result is not needed */ }
            }
        }
    }

    fun setVisibilityTimeout(queue: String, msgIds: List<Long>, timeout: Duration) {
        if (msgIds.isEmpty()) return
        connections.use { conn ->
            conn.prepareStatement("SELECT msg_id FROM pgmq.set_vt(?, ?, ?)").use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.setArray(2, conn.createArrayOf("bigint", msgIds.toTypedArray()))
                ps.setInt(3, timeout.inWholeSeconds.toInt())
                ps.executeQuery().use { }
            }
        }
    }

    private fun callVoid(sql: String, queue: String) {
        connections.use { conn ->
            conn.prepareStatement(sql).use { ps ->
                ps.setString(1, Identifiers.requireValidQueueName(queue))
                ps.execute()
            }
        }
    }

    private fun <T> ResultSet.toMessages(mapPayload: (String) -> T): List<PgmqMessage<T>> = buildList {
        while (next()) {
            val headersJson = getString("headers")
            val payloadJson = getString("message")
            add(
                PgmqMessage(
                    msgId = getLong("msg_id"),
                    readCount = getInt("read_ct"),
                    enqueuedAt = instant("enqueued_at")!!,
                    lastReadAt = instant("last_read_at"),
                    visibleAt = instant("vt")!!,
                    payload = mapPayload(payloadJson ?: "null"),
                    headers = codec.decodeUserHeaders(headersJson),
                    envelope = codec.decode(headersJson),
                    group = codec.decodeGroup(headersJson),
                    diagnostics = codec.decodeDiagnostics(headersJson),
                ),
            )
        }
    }

    private fun ResultSet.toQueueMetrics() = QueueMetrics(
        name = getString("queue_name"),
        length = getLong("queue_length"),
        visibleLength = getLong("queue_visible_length"),
        newestMessageAgeSeconds = getInt("newest_msg_age_sec").takeUnless { wasNull() },
        oldestMessageAgeSeconds = getInt("oldest_msg_age_sec").takeUnless { wasNull() },
        totalMessages = getLong("total_messages"),
        scrapedAt = instant("scrape_time")!!,
    )

    /** Read as [OffsetDateTime]; a `Timestamp` would apply the JVM time zone. */
    private fun ResultSet.instant(column: String): Instant? =
        getObject(column, OffsetDateTime::class.java)?.toInstant()

    companion object {
        const val DEFAULT_SOURCE_ID: String = "unknown"

        /**
         * Used when no target is given; means "not addressed to anyone in particular".
         */
        const val DEFAULT_TARGET_ID: String = "any"

        val DEFAULT_VISIBILITY_TIMEOUT: Duration = 30.seconds
    }
}

// Reified conveniences: `template.read<OrderDto>("orders")` instead of passing a PgmqType.

inline fun <reified T> PgmqTemplate.read(
    queue: String,
    visibilityTimeout: Duration = PgmqTemplate.DEFAULT_VISIBILITY_TIMEOUT,
    quantity: Int = 1,
    conditional: String? = null,
): List<PgmqMessage<T>> = read(queue, pgmqType<T>(), visibilityTimeout, quantity, conditional)

inline fun <reified T> PgmqTemplate.readGrouped(
    queue: String,
    visibilityTimeout: Duration = PgmqTemplate.DEFAULT_VISIBILITY_TIMEOUT,
    quantity: Int = 1,
): List<PgmqMessage<T>> = readGrouped(queue, pgmqType<T>(), visibilityTimeout, quantity)

inline fun <reified T> PgmqTemplate.readGroupedRoundRobin(
    queue: String,
    visibilityTimeout: Duration = PgmqTemplate.DEFAULT_VISIBILITY_TIMEOUT,
    quantity: Int = 1,
): List<PgmqMessage<T>> = readGroupedRoundRobin(queue, pgmqType<T>(), visibilityTimeout, quantity)

inline fun <reified T> PgmqTemplate.readWithPoll(
    queue: String,
    visibilityTimeout: Duration = PgmqTemplate.DEFAULT_VISIBILITY_TIMEOUT,
    quantity: Int = 1,
    maxPoll: Duration = 5.seconds,
    pollInterval: Duration = 100.milliseconds,
    conditional: String? = null,
): List<PgmqMessage<T>> =
    readWithPoll(queue, pgmqType<T>(), visibilityTimeout, quantity, maxPoll, pollInterval, conditional)

inline fun <reified T> PgmqTemplate.pop(
    queue: String,
    quantity: Int = 1,
): List<PgmqMessage<T>> = pop(queue, pgmqType<T>(), quantity)
