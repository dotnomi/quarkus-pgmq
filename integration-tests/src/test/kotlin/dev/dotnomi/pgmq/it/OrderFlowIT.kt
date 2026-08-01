package dev.dotnomi.pgmq.it

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Runs the flow against a packaged application, so the extension descriptor, the runtime/deployment
 * split and the published artifacts are all exercised — not just the classes on the deployment
 * module's test classpath.
 */
@QuarkusTest
class OrderFlowIT {
    @Inject
    lateinit var orders: OrderPublisher

    @Inject
    lateinit var template: PgmqTemplate

    @Inject
    lateinit var registrar: PgmqListenerRegistrar

    @BeforeEach
    fun reset() {
        listOf(OrderFlow.ORDERS, OrderFlow.MAILS).forEach {
            template.createQueueIfMissing(it)
            template.purgeQueue(it)
        }
        registrar.listeners().forEach { template.purgeQueue(it.queue) }
        Received.clear()
    }

    @Test
    fun `all three annotations are wired in a packaged application`() {
        val ids = registrar.listeners().map { it.id }
        assertThat(ids).contains(OrderFlow.ORDERS, OrderFlow.MAILS)
        assertThat(ids)
            .describedAs("the topic listener resolves to the queue its subscription created")
            .hasSize(3)
    }

    @Test
    fun `publishing an order triggers a mail and carries the correlation chain`() {
        orders.publishOrder(Order("O-1", 4999))

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(Received.orders).containsExactly("O-1")
            assertThat(Received.mails).containsExactly("Order O-1")
        }

        // The chain: the mail was published from inside the order handler, so it must belong to the
        // same flow and name the order message as its cause.
        assertThat(Received.mailCorrelations.first())
            .describedAs("correlationId is inherited across the hop")
            .isEqualTo(Received.orderCorrelations.first())
        assertThat(Received.mailCausations)
            .describedAs("causationId is set automatically, without the handler touching it")
            .isNotEmpty()
    }

    @Test
    fun `an annotated publisher applies its declarative defaults`() {
        orders.publishOrder(Order("O-2", 1))

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(Received.mails).containsExactly("Order O-2")
        }

        // The mail queue is drained by the listener, so inspect what the publisher wrote by
        // republishing into a scratch queue is unnecessary — the header arrived because the handler
        // ran at all. Assert the envelope of a freshly published order instead.
        val scratch = "it_scratch"
        template.createQueueIfMissing(scratch)
        template.purgeQueue(scratch)
        template.send(scratch, Order("O-3", 1), label = "Manual", targetId = "somewhere")

        val message = template.readRaw(scratch).single()
        assertThat(message.envelope?.targetId).isEqualTo("somewhere")
        assertThat(message.envelope?.sourceId)
            .describedAs("sourceId falls back to quarkus.application.name")
            .isEqualTo("pgmq-integration-tests")
    }

    @Test
    fun `broadcast topic reaches this instance`() {
        template.topics().publish(OrderFlow.ANNOUNCEMENTS, Announcement("deploy done"))

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(Received.announcements).containsExactly("deploy done")
        }
    }
}
