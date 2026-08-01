package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class PgmqListenerRegistrarTest {
    private val template = PgmqTestDatabase.template("registrar-test")

    private fun <R> withRegistrar(block: (PgmqListenerRegistrar, MutableList<String>) -> R): R {
        val registrar = PgmqListenerRegistrar(template)
        val queues = mutableListOf<String>()
        return try {
            block(registrar, queues)
        } finally {
            runCatching { registrar.stopAll() }
            queues.forEach { queue ->
                runCatching { template.dropQueue(queue) }
                runCatching { template.dropQueue("${queue}_dlq") }
            }
        }
    }

    @Test
    fun `id is the queue name for the default client`() {
        assertThat(ListenerSpec(queue = "mails").id).isEqualTo("mails")
        assertThat(ListenerSpec(queue = "mails", client = "analytics").id).isEqualTo("analytics/mails")
    }

    @Test
    fun `listeners are startable and stoppable by id`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue
            val received = ConcurrentLinkedQueue<String>()

            val id = registrar.register(
                ListenerSpec(queue = queue, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto> { received += it.orderId },
            )
            assertThat(id).isEqualTo(queue)

            registrar.startAll()
            assertThat(registrar.isRunning(id)).isTrue()

            template.send(queue, OrderDto("A-1", 1, emptyList()))
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("A-1")
            }

            registrar.stop(id)
            assertThat(registrar.state(id)).isEqualTo(ListenerState.STOPPED)

            // After a stop the messages stay in the queue instead of moving to the DLQ.
            template.send(queue, OrderDto("A-2", 1, emptyList()))
            Thread.sleep(800)
            assertThat(received).containsExactly("A-1")
            assertThat(template.metrics(queue).length).isEqualTo(1)

            registrar.start(id)
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("A-1", "A-2")
            }
        }
    }

    @Test
    fun `autoStart false shows up as NOT_STARTED and reads nothing`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue
            val received = ConcurrentLinkedQueue<String>()

            val id = registrar.register(
                ListenerSpec(queue = queue, pollInterval = 100.milliseconds, autoStart = false),
                pgmqPayloadHandler<OrderDto> { received += it.orderId },
            )

            registrar.startAll()

            assertThat(registrar.state(id)).isEqualTo(ListenerState.NOT_STARTED)
            assertThat(registrar.listeners().single().autoStart).isFalse()

            template.createQueueIfMissing(queue)
            template.send(queue, OrderDto("B-1", 1, emptyList()))
            Thread.sleep(800)
            assertThat(received).isEmpty()

            registrar.start(id)
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("B-1")
            }
        }
    }

    @Test
    fun `listeners lists all containers with their handlers`() {
        withRegistrar { registrar, queues ->
            val first = PgmqTestDatabase.uniqueQueueName("aaa")
            val second = PgmqTestDatabase.uniqueQueueName("bbb")
            queues += listOf(first, second)

            registrar.register(
                ListenerSpec(queue = first, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto>(label = "X") { },
            )
            registrar.register(
                ListenerSpec(queue = first, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto>(label = "Y") { },
            )
            registrar.register(
                ListenerSpec(queue = second, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto> { },
            )
            registrar.startAll()

            val listeners = registrar.listeners()
            assertThat(listeners).hasSize(2)

            val firstInfo = listeners.single { it.queue == first }
            assertThat(firstInfo.handlers.map { it.label })
                .describedAs("beide Labels teilen sich EINEN Container")
                .containsExactlyInAnyOrder("X", "Y")

            val secondInfo = listeners.single { it.queue == second }
            assertThat(secondInfo.handlers.map { it.label }).containsExactly(null)
        }
    }

    @Test
    fun `contradicting per-queue settings from two handlers are rejected`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue

            registrar.register(
                ListenerSpec(queue = queue, concurrency = 2, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto>(label = "A") { },
            )

            assertThatThrownBy {
                registrar.register(
                    ListenerSpec(queue = queue, concurrency = 8, pollInterval = 100.milliseconds),
                    pgmqPayloadHandler<OrderDto>(label = "B") { },
                )
            }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Conflicting settings")
                .hasMessageContaining("concurrency")
        }
    }

    @Test
    fun `pause and resume work through the registrar`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue
            val received = ConcurrentLinkedQueue<String>()

            val id = registrar.register(
                ListenerSpec(queue = queue, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto> { received += it.orderId },
            )
            registrar.startAll()

            registrar.pause(id)
            assertThat(registrar.state(id)).isEqualTo(ListenerState.PAUSED)
            template.send(queue, OrderDto("C-1", 1, emptyList()))
            Thread.sleep(800)
            assertThat(received).isEmpty()

            registrar.resume(id)
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("C-1")
            }
        }
    }

    @Test
    fun `unknown id gives a helpful error`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue
            registrar.register(
                ListenerSpec(queue = queue, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto> { },
            )
            registrar.startAll()

            assertThatThrownBy { registrar.start("does-not-exist") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("No listener with id")
                .hasMessageContaining(queue)
        }
    }

    @Test
    fun `registering after startAll takes effect immediately`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue
            val received = ConcurrentLinkedQueue<String>()

            registrar.startAll()

            registrar.register(
                ListenerSpec(queue = queue, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto> { received += it.orderId },
            )

            template.send(queue, OrderDto("D-1", 1, emptyList()))
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("D-1")
            }
        }
    }

    @Test
    fun `unregister stops the container`() {
        withRegistrar { registrar, queues ->
            val queue = PgmqTestDatabase.uniqueQueueName("reg")
            queues += queue

            val id = registrar.register(
                ListenerSpec(queue = queue, pollInterval = 100.milliseconds),
                pgmqPayloadHandler<OrderDto> { },
            )
            registrar.startAll()
            assertThat(registrar.listeners()).hasSize(1)

            registrar.unregister(id)
            assertThat(registrar.listeners()).isEmpty()
        }
    }
}
