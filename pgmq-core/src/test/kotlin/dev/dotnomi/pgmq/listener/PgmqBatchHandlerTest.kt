package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.PgmqMessage
import dev.dotnomi.pgmq.serializer.pgmqType
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

/**
 * Regression cover for a defect that 115 green tests missed: `batch = true` was accepted, documented
 * and reported in `HandlerInfo`, but the container still invoked every handler with a one-element
 * list. A batch handler never saw a batch.
 */
class PgmqBatchHandlerTest {
    private val template = PgmqTestDatabase.template("batch-test")

    private fun batchHandler(
        label: String? = null,
        received: ConcurrentLinkedQueue<List<String>>,
    ): RegisteredHandler<OrderDto> = RegisteredHandler(
        label = label,
        payloadType = pgmqType<OrderDto>(),
        batch = true,
        name = label ?: "batch",
        invoke = { messages: List<PgmqMessage<OrderDto>>, _ ->
            received += messages.map { it.payload.orderId }
        },
    )

    private fun <R> withContainer(
        spec: ListenerSpec,
        handlers: List<RegisteredHandler<*>>,
        block: (PgmqListenerContainer) -> R,
    ): R {
        val container = PgmqListenerContainer(spec, template, handlers)
        return try {
            block(container)
        } finally {
            runCatching { container.stop() }
            runCatching { template.dropQueue(spec.queue) }
            runCatching { template.dropQueue(spec.effectiveDeadLetterQueue) }
        }
    }

    @Test
    fun `a batch handler receives the whole read in one call`() {
        val queue = PgmqTestDatabase.uniqueQueueName("bat")
        val received = ConcurrentLinkedQueue<List<String>>()

        withContainer(
            ListenerSpec(queue = queue, batchSize = 5, pollInterval = 100.milliseconds, concurrency = 1),
            listOf(batchHandler(received = received)),
        ) { container ->
            template.createQueueIfMissing(queue)
            template.sendBatch(queue, List(5) { OrderDto("B-$it", it.toLong(), emptyList()) })
            container.start()

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received.sumOf { it.size }).isEqualTo(5)
            }

            assertThat(received)
                .describedAs("one invocation with five messages, not five invocations with one")
                .anySatisfy { assertThat(it).hasSizeGreaterThan(1) }

            // And they really were acknowledged as a batch.
            await().atMost(10.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(queue).length).isZero()
            }
        }
    }

    @Test
    fun `a batch handler failure retries every message of the batch`() {
        val queue = PgmqTestDatabase.uniqueQueueName("bat")
        val attempts = ConcurrentLinkedQueue<Int>()

        val failing = RegisteredHandler(
            label = null,
            payloadType = pgmqType<OrderDto>(),
            batch = true,
            name = "failing",
            invoke = { messages: List<PgmqMessage<OrderDto>>, _ ->
                attempts += messages.size
                error("batch rejected")
            },
        )

        withContainer(
            ListenerSpec(
                queue = queue,
                batchSize = 3,
                pollInterval = 100.milliseconds,
                maxRetries = 1,
                maxRetryBackoff = 1.seconds,
                visibilityTimeout = 60.seconds,
            ),
            listOf(failing),
        ) { container ->
            template.createQueueIfMissing(queue)
            template.sendBatch(queue, List(3) { OrderDto("F-$it", it.toLong(), emptyList()) })
            container.start()

            // maxRetries = 1 means two deliveries, then all three land in the dead letter queue.
            await().atMost(30.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics("${queue}_dlq").length).isEqualTo(3)
            }
            assertThat(attempts).isNotEmpty()
        }
    }

    @Test
    fun `mixing batch and single-message handlers on one queue is rejected`() {
        val received = ConcurrentLinkedQueue<List<String>>()

        assertThatThrownBy {
            PgmqListenerContainer(
                ListenerSpec(queue = "mixed_modes"),
                template,
                listOf(
                    batchHandler(label = "A", received = received),
                    pgmqPayloadHandler<OrderDto>(label = "B") { },
                ),
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .describedAs("pacing, vtRefresh and the FIFO abort are defined per unit of work")
            .hasMessageContaining("mixes batch and single-message handlers")
    }

    @Test
    fun `batch mode still routes by label`() {
        val queue = PgmqTestDatabase.uniqueQueueName("bat")
        val created = ConcurrentLinkedQueue<List<String>>()
        val cancelled = ConcurrentLinkedQueue<List<String>>()

        withContainer(
            ListenerSpec(queue = queue, batchSize = 10, pollInterval = 100.milliseconds),
            listOf(
                batchHandler(label = "Created", received = created),
                batchHandler(label = "Cancelled", received = cancelled),
            ),
        ) { container ->
            template.createQueueIfMissing(queue)
            template.send(queue, OrderDto("C-1", 1, emptyList()), label = "Created")
            template.send(queue, OrderDto("C-2", 1, emptyList()), label = "Cancelled")
            template.send(queue, OrderDto("C-3", 1, emptyList()), label = "Created")
            container.start()

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(created.flatten()).containsExactlyInAnyOrder("C-1", "C-3")
                assertThat(cancelled.flatten()).containsExactly("C-2")
            }
        }
    }
}
