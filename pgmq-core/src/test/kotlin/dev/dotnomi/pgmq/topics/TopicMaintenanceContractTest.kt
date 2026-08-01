package dev.dotnomi.pgmq.topics

import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Pins down the contract between heartbeat and janitor.
 *
 * These two are useless apart, and getting the pairing wrong is dangerous in both directions: without
 * a heartbeat the janitor reclaims the queues of healthy instances, and without a janitor every
 * crashed instance leaves its queue behind for good. Both halves existed and were individually
 * tested, yet nothing in production ever called them — hence this explicit contract.
 */
class TopicMaintenanceContractTest {

    private val template = PgmqTestDatabase.template("maintenance-test")
    private val topics = template.topics()

    init {
        topics.initialiseSchema()
    }

    private fun <R> withTopic(block: (String) -> R): R {
        val topic = "mt_${UUID.randomUUID().toString().replace("-", "").take(12)}"
        return try {
            topics.createTopic(topic)
            block(topic)
        } finally {
            runCatching {
                topics.subscriptions(topic).forEach { runCatching { template.dropQueue(it.queue) } }
                topics.dropTopic(topic)
            }
        }
    }

    @Test
    fun `a heartbeat keeps a live instance from being reclaimed`() {
        withTopic { topic ->
            val live = topics.subscribe(topic, "grp", SubscriptionMode.BROADCAST, instanceId = "live")

            // Age the subscription past the threshold, then do exactly what the maintenance loop does.
            ageSubscription(topic, live.subscriber)
            topics.heartbeat(topic, live.subscriber)

            val reaped = topics.reapStaleSubscriptions(staleAfter = 2.seconds)

            assertThat(reaped)
                .describedAs("the heartbeat must make the janitor skip this instance")
                .doesNotContain(live.queue)
            assertThat(template.queueExists(live.queue)).isTrue()
            assertThat(topics.subscriptions(topic).map { it.subscriber }).contains(live.subscriber)
        }
    }

    @Test
    fun `without a heartbeat the janitor reclaims the queue`() {
        withTopic { topic ->
            val dead = topics.subscribe(topic, "grp", SubscriptionMode.BROADCAST, instanceId = "dead")

            ageSubscription(topic, dead.subscriber)
            // Deliberately no heartbeat — this is the crashed instance.

            val reaped = topics.reapStaleSubscriptions(staleAfter = 2.seconds)

            assertThat(reaped).contains(dead.queue)
            assertThat(template.queueExists(dead.queue)).isFalse()
        }
    }

    @Test
    fun `shared subscriptions are never reclaimed`() {
        withTopic { topic ->
            val shared = topics.subscribe(topic, "billing", SubscriptionMode.SHARED)
            ageSubscription(topic, shared.subscriber)

            val reaped = topics.reapStaleSubscriptions(staleAfter = 2.seconds)

            assertThat(reaped)
                .describedAs(
                    "a shared queue belongs to the group, not to one instance — reclaiming it would " +
                        "throw away messages other pods are still meant to process",
                )
                .doesNotContain(shared.queue)
            assertThat(template.queueExists(shared.queue)).isTrue()

            template.dropQueue(shared.queue)
        }
    }

    @Test
    fun `the janitor is safe to run from every instance at once`() {
        withTopic { topic ->
            val dead = topics.subscribe(topic, "grp", SubscriptionMode.BROADCAST, instanceId = "gone")
            ageSubscription(topic, dead.subscriber)

            // Two "instances" reaping concurrently. The advisory lock means one does the work and the
            // other simply finds nothing — neither may fail.
            val results = listOf(
                PgmqTestDatabase.template().topics(),
                PgmqTestDatabase.template().topics(),
            ).map { runCatching { it.reapStaleSubscriptions(staleAfter = 2.seconds) } }

            assertThat(results).allSatisfy { assertThat(it.isSuccess).isTrue() }
            assertThat(results.flatMap { it.getOrDefault(emptyList()) }.filter { it == dead.queue })
                .describedAs("the queue is reclaimed exactly once, not once per instance")
                .containsExactly(dead.queue)
        }
    }

    private fun ageSubscription(topic: String, subscriber: String) {
        PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.prepareStatement(
                "UPDATE pgmq_topics.subscription SET last_seen_at = now() - interval '1 hour' " +
                    "WHERE topic = ? AND subscriber = ?",
            ).use { ps ->
                ps.setString(1, topic)
                ps.setString(2, subscriber)
                ps.execute()
            }
        }
    }
}
