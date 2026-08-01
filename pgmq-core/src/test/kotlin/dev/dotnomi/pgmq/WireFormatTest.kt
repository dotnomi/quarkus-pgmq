package dev.dotnomi.pgmq

import com.fasterxml.jackson.databind.ObjectMapper
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins down what a sent message actually looks like in the database.
 *
 * This is the contract other systems read and write against, so it is worth an explicit test rather
 * than being an accident of the implementation. `message` holds the payload untouched; everything the
 * library adds lives in `headers`.
 */
class WireFormatTest {

    private val template = PgmqTestDatabase.template("wire-format")
    private val mapper = ObjectMapper()

    data class MailDto(val recipient: String, val text: String)

    private fun <R> onQueue(block: (String) -> R): R {
        val queue = PgmqTestDatabase.uniqueQueueName("wire")
        template.createQueueIfMissing(queue)
        return try {
            block(queue)
        } finally {
            runCatching { template.dropQueue(queue) }
        }
    }

    /** Reads the two jsonb columns straight out of the queue table, bypassing the library. */
    private fun rawRow(queue: String): Pair<String, String> =
        PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.prepareStatement(
                "SELECT jsonb_pretty(message), jsonb_pretty(headers) FROM pgmq.q_$queue ORDER BY msg_id LIMIT 1",
            ).use { ps ->
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "no message in $queue" }
                    rs.getString(1) to rs.getString(2)
                }
            }
        }

    @Test
    fun `the payload is stored untouched and every addition lives in headers`() {
        onQueue { queue ->
            template.send(
                queue = queue,
                payload = MailDto("ada@example.com", "hello"),
                label = "SendMail",
                targetId = "mailer",
                headers = mapOf("priority" to "high"),
            )

            val (message, headers) = rawRow(queue)
            println("message:\n$message\n\nheaders:\n$headers")

            val body = mapper.readTree(message)
            assertThat(body.fieldNames().asSequence().toList())
                .describedAs("the payload carries no library fields — it is exactly the serialized object")
                .containsExactlyInAnyOrder("recipient", "text")

            val envelope = mapper.readTree(headers)
            assertThat(envelope.fieldNames().asSequence().toList())
                .describedAs("the two optional fields, label and causationId, are absent rather than null")
                .containsExactlyInAnyOrder(
                    "messageId", "sourceId", "targetId", "label",
                    "correlationId", "schemaVersion", "sendingTime",
                    "priority",
                )
            assertThat(envelope["label"].asText()).isEqualTo("SendMail")
            assertThat(envelope["targetId"].asText()).isEqualTo("mailer")
            assertThat(envelope.has("causationId"))
                .describedAs("start of a chain — nothing caused this message, so the key is omitted")
                .isFalse()
            assertThat(envelope["schemaVersion"].asInt()).isEqualTo(1)
            assertThat(envelope["priority"].asText())
                .describedAs("a user header sits flat next to the envelope, not nested")
                .isEqualTo("high")
            assertThat(envelope["sendingTime"].asText()).matches("""\d{4}-\d{2}-\d{2}T.*Z""")
        }
    }

    @Test
    fun `a FIFO group adds the header pgmq itself reads`() {
        onQueue { queue ->
            template.send(queue, MailDto("ada@example.com", "hi"), group = "ada")

            val (_, headers) = rawRow(queue)
            assertThat(mapper.readTree(headers)["x-pgmq-group"].asText())
                .describedAs("x-pgmq-group is pgmq's own key, not ours — it is what read_grouped matches on")
                .isEqualTo("ada")
        }
    }

    @Test
    fun `a payload that is not an object is stored as that JSON value`() {
        onQueue { queue ->
            template.send(queue, listOf(1, 2, 3))

            val (message, _) = rawRow(queue)
            assertThat(mapper.readTree(message).isArray)
                .describedAs("message is jsonb, so any JSON value works — not just objects")
                .isTrue()
        }
    }
}
