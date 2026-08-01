package dev.dotnomi.pgmq.listener

import com.fasterxml.jackson.databind.JsonNode
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * What a handler may declare as its payload type, and what each choice means for a given message
 * shape. The distinction matters when the mapping should stay in application code rather than
 * happening automatically.
 */
class PayloadTypeTest {

    private val template = PgmqTestDatabase.template("payload-type")

    private fun withListener(
        handler: RegisteredHandler<*>,
        payload: Any,
        block: (dlqLength: () -> Long) -> Unit,
    ) {
        val queue = PgmqTestDatabase.uniqueQueueName("pt")
        val spec = ListenerSpec(queue = queue, pollInterval = 100.milliseconds, maxRetries = 0)
        val container = PgmqListenerContainer(spec, template, listOf(handler))
        try {
            template.createQueueIfMissing(queue)
            template.send(queue, payload)
            container.start()
            block { runCatching { template.metrics(spec.effectiveDeadLetterQueue).length }.getOrDefault(0) }
        } finally {
            runCatching { container.stop() }
            runCatching { template.dropQueue(queue) }
            runCatching { template.dropQueue(spec.effectiveDeadLetterQueue) }
        }
    }

    @Test
    fun `a String handler receives a JSON string body unquoted`() {
        val seen = ConcurrentLinkedQueue<String>()
        withListener(pgmqPayloadHandler<String> { seen += it }, "just a string") {
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(seen).containsExactly("just a string")
            }
        }
    }

    @Test
    fun `a String handler cannot take a JSON object body`() {
        val seen = ConcurrentLinkedQueue<String>()
        withListener(pgmqPayloadHandler<String> { seen += it }, mapOf("a" to 1)) { dlqLength ->
            // Deserializing an object into a String is impossible, so this is a permanent failure.
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(dlqLength()).isEqualTo(1)
            }
            assertThat(seen).isEmpty()
        }
    }

    @Test
    fun `a raw handler receives any shape verbatim`() {
        val seen = ConcurrentLinkedQueue<String>()
        withListener(pgmqRawHandler { seen += it }, mapOf("a" to 1, "b" to "two")) {
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(seen).hasSize(1)
            }
            assertThat(seen.single())
                .describedAs("the stored JSON text, not a parsed object")
                .contains("\"a\"").contains("\"b\"").contains("two")
        }
    }

    @Test
    fun `a raw handler also receives a plain string body, quoted as stored`() {
        val seen = ConcurrentLinkedQueue<String>()
        withListener(pgmqRawHandler { seen += it }, "just a string") {
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(seen).containsExactly("\"just a string\"")
            }
        }
    }

    @Test
    fun `a JsonNode handler takes any shape without committing to a class`() {
        val seen = ConcurrentLinkedQueue<String>()
        withListener(pgmqPayloadHandler<JsonNode> { seen += it["a"].asText() }, mapOf("a" to "value")) {
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(seen).containsExactly("value")
            }
        }
    }
}
