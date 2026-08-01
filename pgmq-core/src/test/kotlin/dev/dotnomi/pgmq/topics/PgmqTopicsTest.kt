package dev.dotnomi.pgmq.topics

import dev.dotnomi.pgmq.read
import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Covers the distinction that motivated topics in the first place: three replicas of one application
 * that must *all* see a message, versus three replicas that must share the work.
 */
class PgmqTopicsTest {

    private val template = PgmqTestDatabase.template("topic-test")
    private val topics = template.topics()

    init {
        topics.initialiseSchema()
    }

    private fun <R> withTopic(block: (String) -> R): R {
        val topic = "tp_${UUID.randomUUID().toString().replace("-", "").take(12)}"
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
    fun `broadcast delivers the same message to every instance`(): Unit = withTopic { topic ->
        // Three pods of one application.
        val pods = listOf("pod-a", "pod-b", "pod-c").map {
            topics.subscribe(topic, group = "notifier", mode = SubscriptionMode.BROADCAST, instanceId = it)
        }
        assertThat(pods.map { it.queue }.distinct())
            .describedAs("each instance gets its own queue")
            .hasSize(3)

        topics.publish(topic, OrderDto("B-1", 1, emptyList()), label = "Broadcast")

        pods.forEach { pod ->
            val messages = template.read<OrderDto>(pod.queue, quantity = 10)
            assertThat(messages)
                .describedAs("instance %s must have received the message", pod.subscriber)
                .hasSize(1)
            assertThat(messages.single().payload.orderId).isEqualTo("B-1")
        }
    }

    @Test
    fun `shared delivers the message once per group`(): Unit = withTopic { topic ->
        // All three pods register the same shared subscription — the normal case where every pod
        // runs the same startup code.
        val subscriptions = listOf("pod-a", "pod-b", "pod-c").map {
            topics.subscribe(topic, group = "billing", mode = SubscriptionMode.SHARED, instanceId = it)
        }

        assertThat(subscriptions.map { it.queue }.distinct())
            .describedAs("a shared group uses exactly one queue no matter how many pods register")
            .hasSize(1)

        topics.publish(topic, OrderDto("S-1", 1, emptyList()))

        val queue = subscriptions.first().queue
        assertThat(template.metrics(queue).length)
            .describedAs("exactly one copy, which the pods then compete for")
            .isEqualTo(1)
    }

    @Test
    fun `both modes side by side each get their own copy`(): Unit = withTopic { topic ->
        val shared = topics.subscribe(topic, group = "billing", mode = SubscriptionMode.SHARED)
        val podA = topics.subscribe(topic, "notifier", SubscriptionMode.BROADCAST, instanceId = "pod-a")
        val podB = topics.subscribe(topic, "notifier", SubscriptionMode.BROADCAST, instanceId = "pod-b")

        val written = topics.publish(topic, OrderDto("M-1", 1, emptyList()))
        assertThat(written).hasSize(3)

        listOf(shared.queue, podA.queue, podB.queue).forEach { queue ->
            assertThat(template.metrics(queue).length).isEqualTo(1)
        }
    }

    @Test
    fun `fan-out is atomic`(): Unit = withTopic { topic ->
        val first = topics.subscribe(topic, "a", SubscriptionMode.BROADCAST, instanceId = "i1")
        val second = topics.subscribe(topic, "b", SubscriptionMode.BROADCAST, instanceId = "i2")

        // Drop one target queue behind the registry's back, so the fan-out fails halfway through.
        template.dropQueue(second.queue)

        runCatching { topics.publish(topic, OrderDto("A-1", 1, emptyList())) }
            .also { assertThat(it.isFailure).isTrue() }

        assertThat(template.metrics(first.queue).length)
            .describedAs("a partial fan-out would leave subscribers permanently out of sync")
            .isZero()
    }

    @Test
    fun `publishing to a topic without subscribers delivers nowhere`(): Unit = withTopic { topic ->
        assertThat(topics.publish(topic, OrderDto("N-1", 1, emptyList()))).isEmpty()
    }

    @Test
    fun `unsubscribe drops an ephemeral queue but keeps a shared one`(): Unit = withTopic { topic ->
        val shared = topics.subscribe(topic, "billing", SubscriptionMode.SHARED)
        val pod = topics.subscribe(topic, "notifier", SubscriptionMode.BROADCAST, instanceId = "pod-a")

        topics.unsubscribe(topic, "notifier", instanceId = "pod-a")
        assertThat(template.queueExists(pod.queue))
            .describedAs("a per-instance queue is useless once its instance is gone")
            .isFalse()

        topics.unsubscribe(topic, "billing")
        assertThat(template.queueExists(shared.queue))
            .describedAs("a shared queue may still hold messages for other pods, so it stays")
            .isTrue()

        template.dropQueue(shared.queue)
    }

    @Test
    fun `janitor reclaims queues whose instance died without unsubscribing`(): Unit = withTopic { topic ->
        val alive = topics.subscribe(topic, "grp", SubscriptionMode.BROADCAST, instanceId = "alive")
        val crashed = topics.subscribe(topic, "grp", SubscriptionMode.BROADCAST, instanceId = "crashed")

        // Simulate a crash: the heartbeat stops. The living pod keeps its own current.
        expireHeartbeat(topic, crashed.subscriber)
        topics.heartbeat(topic, alive.subscriber)

        val reaped = topics.reapStaleSubscriptions(staleAfter = 1.seconds)

        // `contains`, not `containsExactly`: the janitor is database-wide by design and may also
        // reclaim stale subscriptions left behind by other tests or earlier runs.
        assertThat(reaped).contains(crashed.queue)
        assertThat(template.queueExists(crashed.queue)).isFalse()
        assertThat(template.queueExists(alive.queue))
            .describedAs("the healthy instance must not lose its queue")
            .isTrue()
        assertThat(topics.subscriptions(topic).map { it.subscriber }).containsExactly(alive.subscriber)
    }

    @Test
    fun `subscriptions and topics can be listed`(): Unit = withTopic { topic ->
        topics.subscribe(topic, "billing", SubscriptionMode.SHARED)
        topics.subscribe(topic, "notifier", SubscriptionMode.BROADCAST, instanceId = "pod-a")

        val subscriptions = topics.subscriptions(topic)
        assertThat(subscriptions).hasSize(2)
        assertThat(subscriptions.map { it.mode })
            .containsExactlyInAnyOrder(SubscriptionMode.SHARED, SubscriptionMode.BROADCAST)

        assertThat(topics.topics().map { it.name }).contains(topic)
    }

    @Test
    fun `queue names stay within the pgmq length limit`() {
        val long = TopicQueueNaming.queueNameFor(
            topic = "a-very-long-topic-name-that-goes-on-and-on",
            subscriber = "some-application__pod-with-a-long-generated-identifier-12345",
        )
        assertThat(long.length).isLessThanOrEqualTo(47)

        // Deterministic, and different inputs must not collapse onto the same name even though both
        // get truncated at the same point.
        val other = TopicQueueNaming.queueNameFor(
            topic = "a-very-long-topic-name-that-goes-on-and-on",
            subscriber = "some-application__pod-with-a-long-generated-identifier-99999",
        )
        assertThat(long).isEqualTo(
            TopicQueueNaming.queueNameFor(
                "a-very-long-topic-name-that-goes-on-and-on",
                "some-application__pod-with-a-long-generated-identifier-12345",
            ),
        )
        assertThat(long).isNotEqualTo(other)
    }

    @Test
    fun `fifo group survives the fan-out`(): Unit = withTopic { topic ->
        val subscription = topics.subscribe(topic, "grp", SubscriptionMode.BROADCAST, instanceId = "pod-a")
        template.createFifoIndex(subscription.queue)

        topics.publish(topic, OrderDto("G-1", 1, emptyList()), group = "user-42")

        assertThat(template.read<OrderDto>(subscription.queue).single().group)
            .describedAs("ordering guarantees must not be lost by going through a topic")
            .isEqualTo("user-42")
    }

    private fun expireHeartbeat(topic: String, subscriber: String) {
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
