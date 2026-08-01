package dev.dotnomi.pgmq

import dev.dotnomi.pgmq.envelope.PgmqHeaderNames
import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import dev.dotnomi.pgmq.support.PgmqTestDatabase.withQueue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

/**
 * Pins down the pgmq properties this library's design is built on.
 *
 * These tests do not check our code, they check **assumptions about pgmq**. They fail when an upgrade
 * of the extension changes one of those properties — and at that point the design has to be revisited
 * rather than the test adjusted.
 */
class PgmqBehaviourContractTest {
    private val template = PgmqTestDatabase.template()

    // --- Installation requirements ---

    @Test
    fun `installed pgmq supports headers`() {
        val version = template.extensionVersion()
        assertThat(version).isNotNull()
        val (major, minor) = version!!.split(".").take(2).map { it.toInt() }

        assertThat(major * 1000 + minor)
            .describedAs("the envelope lives in the headers column, which exists from pgmq 1.5.0 on")
            .isGreaterThanOrEqualTo(1 * 1000 + 5)
    }

    @Test
    fun `no pgmq function is SECURITY DEFINER`() {
        // The basis of per-queue permissions: because every function runs with the caller's rights,
        // plain table ACLs take effect. A SECURITY DEFINER function would bypass the permission
        // check and the whole approach would collapse.
        val securityDefiners = PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT p.proname
                FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'pgmq' AND p.prosecdef
                """.trimIndent(),
            ).use { ps ->
                ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
            }
        }

        assertThat(securityDefiners)
            .describedAs("separating rights by GRANT requires SECURITY INVOKER")
            .isEmpty()
    }

    @Test
    fun `metrics_result exposes queue_visible_length`() {
        // Only present from 1.10 on, and the more useful figure of the two for backlog alerting.
        val columns = PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT a.attname
                FROM pg_type t
                JOIN pg_class c ON c.oid = t.typrelid
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = 'pgmq' AND t.typname = 'metrics_result'
                """.trimIndent(),
            ).use { ps ->
                ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
            }
        }
        assertThat(columns).contains("queue_visible_length", "queue_length")
    }

    @Test
    fun `queue names are limited to 47 characters`() {
        // Why the library only pre-checks the length and rejects nothing else itself.
        PgmqTestDatabase.dataSource.connection.use { conn ->
            fun validate(name: String) = runCatching {
                conn.prepareStatement("SELECT pgmq.validate_queue_name(?)").use { ps ->
                    ps.setString(1, name)
                    ps.execute()
                }
            }

            assertThat(validate("a".repeat(47)).isSuccess).isTrue()
            assertThat(validate("a".repeat(48)).isSuccess).isFalse()
            // Deliberately allowed by pgmq — our validation must not be stricter than that.
            assertThat(validate("With-Dash.And.Dots").isSuccess).isTrue()
            assertThat(validate("1leading_digit").isSuccess).isTrue()
        }
    }

    // --- Concurrent reads ---

    @Test
    fun `concurrent readers never receive the same message`(): Unit = withQueue { queue ->
        template.sendBatch(queue, List(50) { OrderDto("N-$it", it.toLong(), emptyList()) })

        val readers = List(4) { PgmqTestDatabase.template() }
        val collected = readers.flatMap { reader ->
            reader.read<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 20).map { it.msgId }
        }

        assertThat(collected)
            .describedAs("FOR UPDATE SKIP LOCKED keeps two readers from getting the same row")
            .doesNotHaveDuplicates()
        assertThat(collected).hasSize(50)
    }

    // --- FIFO groups — what `fifo = true` with `concurrency > 1` rests on ---

    @Test
    fun `read_grouped skips groups that still have messages in flight`(): Unit =
        withQueue(fifo = true) { queue ->
            listOf("A" to "A1", "B" to "B1", "A" to "A2", "B" to "B2", "A" to "A3", "A" to "A4")
                .forEach { (group, body) ->
                    template.send(queue, OrderDto(body, 1, emptyList()), group = group)
                }

            val first = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 2)
            assertThat(first.map { it.group }.distinct())
                .describedAs("one read_grouped returns messages of exactly one group")
                .hasSize(1)
            val firstGroup = first.first().group

            val second = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 2)
            assertThat(second.map { it.group }.distinct()).hasSize(1)
            val secondGroup = second.first().group

            assertThat(secondGroup)
                .describedAs(
                    "the first group still has unacknowledged messages and is skipped — which is " +
                        "exactly why order within a group survives several workers",
                )
                .isNotEqualTo(firstGroup)

            // Both groups are in flight now, so nothing may be handed out even though A3 and A4 sit
            // untouched in the queue.
            val third = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 5)
            assertThat(third)
                .describedAs("A3/A4 are visible but must not be delivered ahead of A1/A2")
                .isEmpty()
        }

    @Test
    fun `read_grouped preserves order within a group`(): Unit = withQueue(fifo = true) { queue ->
        val expected = (1..5).map { "A$it" }
        expected.forEach { template.send(queue, OrderDto(it, 1, emptyList()), group = "A") }

        val received = mutableListOf<String>()
        repeat(5) {
            val batch = template.readGrouped<OrderDto>(queue, quantity = 1)
            batch.forEach { message ->
                received += message.payload.orderId
                template.delete(queue, message.msgId) // acknowledge, or the next one stays blocked
            }
        }

        assertThat(received).containsExactlyElementsOf(expected)
    }

    @Test
    fun `read_grouped_rr spreads one call across groups`(): Unit = withQueue(fifo = true) { queue ->
        listOf("A" to "A1", "B" to "B1", "C" to "C1", "A" to "A2")
            .forEach { (group, body) ->
                template.send(queue, OrderDto(body, 1, emptyList()), group = group)
            }

        val roundRobin = template.readGroupedRoundRobin<OrderDto>(
            queue = queue,
            visibilityTimeout = 300.seconds,
            quantity = 3,
        )

        assertThat(roundRobin.mapNotNull { it.group }.distinct())
            .describedAs(
                "round robin reaches across groups — which leaves nothing for further workers and " +
                    "makes this mode a fit for concurrency = 1",
            )
            .hasSizeGreaterThan(1)
    }

    @Test
    fun `read_grouped inside an open transaction starves other workers`(): Unit =
        withQueue(fifo = true) { queue ->
            template.send(queue, OrderDto("A1", 1, emptyList()), group = "A")
            template.send(queue, OrderDto("B1", 1, emptyList()), group = "B")

            // Why FIFO reads need a short transaction of their own: read_grouped holds a queue-wide
            // lock until the transaction ends.
            PgmqTestDatabase.dataSource.connection.use { held ->
                held.autoCommit = false
                val inTransaction = template.withConnection(held)
                    .readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 1)
                assertThat(inTransaction).hasSize(1)

                val other = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 1)
                assertThat(other)
                    .describedAs(
                        "group B would be free, but the queue-wide lock of the open transaction " +
                            "blocks every further read",
                    )
                    .isEmpty()

                held.rollback()
            }
        }

    // --- Headers ---

    @Test
    fun `pgmq groups by the x-pgmq-group header`(): Unit = withQueue(fifo = true) { queue ->
        template.send(queue, OrderDto("A1", 1, emptyList()), group = "group-1")

        val message = template.readGrouped<OrderDto>(queue, quantity = 1).single()
        assertThat(message.group).isEqualTo("group-1")
        // The group is a reserved header and does not show up among the user headers.
        assertThat(message.headers).doesNotContainKey(PgmqHeaderNames.GROUP)
    }
}
