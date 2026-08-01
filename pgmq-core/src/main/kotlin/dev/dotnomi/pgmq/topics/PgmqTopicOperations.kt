package dev.dotnomi.pgmq.topics

import dev.dotnomi.pgmq.CorrelationPolicy
import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.envelope.PgmqHeaderNames
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/**
 * Topics on top of pgmq: one publish reaches every subscriber.
 *
 * pgmq has no topics, so this maps a topic onto **one real queue per subscriber** and fans out on
 * publish. Nothing about the pgmq extension is modified.
 *
 * The fan-out runs inside a single transaction, so either every subscriber gets the message or none
 * does — a partial fan-out would leave subscribers permanently out of sync with no way to notice.
 *
 * Semantics worth knowing, and documented rather than papered over: a subscriber that is not yet
 * registered misses messages, exactly like SNS or Redis pub/sub. Subscriptions are therefore
 * registered at application start, before the first publish.
 */
class PgmqTopicOperations internal constructor(
    private val template: PgmqTemplate,
) {
    private val log = LoggerFactory.getLogger(PgmqTopicOperations::class.java)

    // ------------------------------------------------------------------------------------------
    // Schema
    // ------------------------------------------------------------------------------------------

    /**
     * Creates the bookkeeping schema. Idempotent, safe to call from every starting pod.
     *
     * Done programmatically rather than through Flyway so the library stays usable in applications
     * that manage their schema differently — or not at all.
     */
    fun initialiseSchema() {
        template.runOnConnection { conn ->
            conn.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS pgmq_topics")
                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pgmq_topics.topic (
                        name       text PRIMARY KEY,
                        created_at timestamptz NOT NULL DEFAULT now()
                    )
                    """.trimIndent(),
                )
                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pgmq_topics.subscription (
                        topic        text        NOT NULL REFERENCES pgmq_topics.topic(name) ON DELETE CASCADE,
                        subscriber   text        NOT NULL,
                        group_name   text        NOT NULL,
                        queue_name   text        NOT NULL,
                        mode         text        NOT NULL,
                        ephemeral    boolean     NOT NULL DEFAULT false,
                        last_seen_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (topic, subscriber)
                    )
                    """.trimIndent(),
                )
                // Publishing resolves subscriptions by topic on every call, so this index is on the
                // hot path.
                statement.execute(
                    "CREATE INDEX IF NOT EXISTS subscription_topic_idx ON pgmq_topics.subscription (topic)",
                )
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Topics and subscriptions
    // ------------------------------------------------------------------------------------------

    fun createTopic(topic: String) {
        template.runOnConnection { conn ->
            conn.prepareStatement(
                "INSERT INTO pgmq_topics.topic (name) VALUES (?) ON CONFLICT DO NOTHING",
            ).use { ps ->
                ps.setString(1, topic)
                ps.execute()
            }
        }
    }

    /** Removes the topic, all its subscriptions and every ephemeral queue behind them. */
    fun dropTopic(topic: String) {
        subscriptions(topic).filter { it.ephemeral }.forEach { runCatching { template.dropQueue(it.queue) } }
        template.runOnConnection { conn ->
            conn.prepareStatement("DELETE FROM pgmq_topics.topic WHERE name = ?").use { ps ->
                ps.setString(1, topic)
                ps.execute()
            }
        }
    }

    /**
     * Registers a subscription and creates the queue behind it.
     *
     * For [SubscriptionMode.SHARED] all pods of [group] share one queue, so calling this from every
     * pod is both expected and harmless. For [SubscriptionMode.BROADCAST] each pod gets its own
     * queue, keyed by [instanceId].
     */
    @JvmOverloads
    fun subscribe(
        topic: String,
        group: String,
        mode: SubscriptionMode = SubscriptionMode.SHARED,
        instanceId: String = InstanceId.current,
    ): TopicSubscription {
        createTopic(topic)

        val subscriber = when (mode) {
            SubscriptionMode.SHARED -> group
            SubscriptionMode.BROADCAST -> "${group}__$instanceId"
        }
        val queue = TopicQueueNaming.queueNameFor(topic, subscriber)
        val ephemeral = mode == SubscriptionMode.BROADCAST

        // The queue exists before the row does: a subscription pointing at a missing queue would
        // make the next publish fail for everyone, not just this subscriber.
        template.createQueueIfMissing(queue)

        template.runOnConnection { conn ->
            conn.prepareStatement(
                """
                INSERT INTO pgmq_topics.subscription
                    (topic, subscriber, group_name, queue_name, mode, ephemeral, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (topic, subscriber) DO UPDATE SET last_seen_at = now()
                """.trimIndent(),
            ).use { ps ->
                ps.setString(1, topic)
                ps.setString(2, subscriber)
                ps.setString(3, group)
                ps.setString(4, queue)
                ps.setString(5, mode.name)
                ps.setBoolean(6, ephemeral)
                ps.execute()
            }
        }

        log.info("Subscribed '{}' to topic '{}' as {} using queue '{}'.", subscriber, topic, mode, queue)
        return TopicSubscription(topic, subscriber, group, queue, mode, ephemeral, Instant.now())
    }

    /** Removes a subscription; ephemeral queues are dropped, shared ones are left alone. */
    fun unsubscribe(topic: String, group: String, instanceId: String = InstanceId.current) {
        val existing = subscriptions(topic)
            .filter { it.group == group && (it.mode == SubscriptionMode.SHARED || it.subscriber.endsWith("__$instanceId")) }

        existing.forEach { subscription ->
            template.runOnConnection { conn ->
                conn.prepareStatement(
                    "DELETE FROM pgmq_topics.subscription WHERE topic = ? AND subscriber = ?",
                ).use { ps ->
                    ps.setString(1, topic)
                    ps.setString(2, subscription.subscriber)
                    ps.execute()
                }
            }
            // A shared queue may still be served by other pods, so only per-instance queues go away.
            if (subscription.ephemeral) runCatching { template.dropQueue(subscription.queue) }
            log.info("Unsubscribed '{}' from topic '{}'.", subscription.subscriber, topic)
        }
    }

    /** Refreshes the heartbeat so the janitor does not reclaim this subscription's queue. */
    fun heartbeat(topic: String, subscriber: String) {
        template.runOnConnection { conn ->
            conn.prepareStatement(
                "UPDATE pgmq_topics.subscription SET last_seen_at = now() WHERE topic = ? AND subscriber = ?",
            ).use { ps ->
                ps.setString(1, topic)
                ps.setString(2, subscriber)
                ps.execute()
            }
        }
    }

    fun topics(): List<TopicInfo> = template.runOnConnection { conn ->
        conn.prepareStatement("SELECT name, created_at FROM pgmq_topics.topic ORDER BY name").use { ps ->
            ps.executeQuery().use { rs ->
                buildList {
                    while (rs.next()) {
                        val name = rs.getString("name")
                        add(
                            TopicInfo(
                                name = name,
                                createdAt = rs.getObject("created_at", java.time.OffsetDateTime::class.java).toInstant(),
                                subscriptions = emptyList(),
                            ),
                        )
                    }
                }
            }
        }
    }.map { it.copy(subscriptions = subscriptions(it.name)) }

    fun subscriptions(topic: String): List<TopicSubscription> = template.runOnConnection { conn ->
        conn.prepareStatement(
            """
            SELECT subscriber, group_name, queue_name, mode, ephemeral, last_seen_at
            FROM pgmq_topics.subscription WHERE topic = ? ORDER BY subscriber
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, topic)
            ps.executeQuery().use { rs ->
                buildList {
                    while (rs.next()) {
                        add(
                            TopicSubscription(
                                topic = topic,
                                subscriber = rs.getString("subscriber"),
                                group = rs.getString("group_name"),
                                queue = rs.getString("queue_name"),
                                mode = SubscriptionMode.valueOf(rs.getString("mode")),
                                ephemeral = rs.getBoolean("ephemeral"),
                                lastSeenAt = rs.getObject("last_seen_at", java.time.OffsetDateTime::class.java)
                                    .toInstant(),
                            ),
                        )
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Publishing
    // ------------------------------------------------------------------------------------------

    /**
     * Fans a message out to every subscriber of [topic].
     *
     * All sends share one transaction: either every subscriber receives the message or none does.
     * A partially delivered fan-out would leave some subscribers permanently behind with nothing to
     * indicate it happened.
     *
     * @return the queues the message was written to, empty when nobody is subscribed
     */
    @JvmOverloads
    fun publish(
        topic: String,
        payload: Any?,
        label: String? = null,
        targetId: String = PgmqTemplate.DEFAULT_TARGET_ID,
        headers: Map<String, String> = emptyMap(),
        group: String? = null,
        correlation: CorrelationPolicy = CorrelationPolicy.INHERIT,
        delay: Duration = Duration.ZERO,
    ): List<String> {
        val targets = subscriptions(topic)

        if (targets.isEmpty()) {
            // Not an error: it is the documented "nobody is listening yet" case. Logged so a silent
            // fan-out to nowhere is at least visible.
            log.warn("Topic '{}' has no subscribers — the message was not delivered anywhere.", topic)
            return emptyList()
        }

        template.inTransaction { tx ->
            targets.forEach { subscription ->
                tx.send(
                    queue = subscription.queue,
                    payload = payload,
                    label = label,
                    targetId = targetId,
                    headers = headers,
                    // The FIFO group travels with the message, so ordering guarantees survive fan-out.
                    group = group,
                    correlation = correlation,
                    delay = delay,
                )
            }
        }

        log.debug("Published to topic '{}' -> {} queue(s).", topic, targets.size)
        return targets.map { it.queue }
    }

    // ------------------------------------------------------------------------------------------
    // Janitor
    // ------------------------------------------------------------------------------------------

    /**
     * Reclaims ephemeral queues whose owner died without unsubscribing.
     *
     * Every pod may call this; a Postgres advisory lock ensures only one actually does the work at a
     * time. That avoids a leader election and stays correct as pods come and go.
     *
     * @return the queues that were removed
     */
    fun reapStaleSubscriptions(staleAfter: Duration = DEFAULT_STALE_AFTER): List<String> =
        template.runOnConnection { conn ->
            if (!tryAdvisoryLock(conn)) {
                log.debug("Another instance is already reaping stale topic subscriptions.")
                return@runOnConnection emptyList()
            }

            try {
                val stale = conn.prepareStatement(
                    """
                    DELETE FROM pgmq_topics.subscription
                    WHERE ephemeral
                      AND last_seen_at < now() - make_interval(secs => ?)
                    RETURNING topic, subscriber, queue_name
                    """.trimIndent(),
                ).use { ps ->
                    ps.setDouble(1, staleAfter.inWholeSeconds.toDouble())
                    ps.executeQuery().use { rs ->
                        buildList {
                            while (rs.next()) {
                                add(Triple(rs.getString(1), rs.getString(2), rs.getString(3)))
                            }
                        }
                    }
                }

                stale.forEach { (topic, subscriber, queue) ->
                    runCatching { template.dropQueue(queue) }
                        .onSuccess {
                            log.info(
                                "Reaped stale subscription '{}' on topic '{}' and dropped queue '{}'.",
                                subscriber, topic, queue,
                            )
                        }
                        .onFailure { log.warn("Could not drop stale queue '{}'.", queue, it) }
                }

                stale.map { it.third }
            } finally {
                releaseAdvisoryLock(conn)
            }
        }

    private fun tryAdvisoryLock(conn: Connection): Boolean =
        conn.prepareStatement("SELECT pg_try_advisory_lock(?)").use { ps ->
            ps.setLong(1, JANITOR_LOCK_KEY)
            ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
        }

    private fun releaseAdvisoryLock(conn: Connection) {
        runCatching {
            conn.prepareStatement("SELECT pg_advisory_unlock(?)").use { ps ->
                ps.setLong(1, JANITOR_LOCK_KEY)
                ps.executeQuery().use { }
            }
        }
    }

    companion object {
        /**
         * Must comfortably exceed the heartbeat interval, otherwise a slow but healthy pod would have
         * its queue pulled out from under it.
         */
        val DEFAULT_STALE_AFTER: Duration = 15.minutes

        /** Arbitrary but fixed, so every instance contends for the same lock. */
        private const val JANITOR_LOCK_KEY: Long = 8_247_113_004_517L
    }
}
