package dev.dotnomi.pgmq

import dev.dotnomi.pgmq.envelope.PgmqHeaderNames
import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import dev.dotnomi.pgmq.support.PgmqTestDatabase.withQueue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

/**
 * Haelt die Eigenschaften von pgmq fest, auf denen der Entwurf dieser Bibliothek aufbaut.
 *
 * Diese Tests pruefen nicht unseren Code, sondern **Annahmen ueber pgmq**. Sie schlagen fehl, wenn
 * ein Upgrade der Extension eine dieser Eigenschaften aendert — und genau dann muss der Entwurf
 * nachgezogen werden, nicht der Test angepasst.
 */
class PgmqBehaviourContractTest {
    private val template = PgmqTestDatabase.template()

    // ------------------------------------------------------------------------------------------
    // Voraussetzungen der Installation
    // ------------------------------------------------------------------------------------------

    @Test
    fun `installed pgmq supports headers`() {
        val version = template.extensionVersion()
        assertThat(version).isNotNull()
        val (major, minor) = version!!.split(".").take(2).map { it.toInt() }

        assertThat(major * 1000 + minor)
            .describedAs("Der Envelope liegt in der headers-Spalte, die es erst ab pgmq 1.5.0 gibt")
            .isGreaterThanOrEqualTo(1 * 1000 + 5)
    }

    @Test
    fun `no pgmq function is SECURITY DEFINER`() {
        // Grundlage der Rechte-Trennung: weil alle Funktionen mit den Rechten des Aufrufers laufen,
        // greifen Tabellen-ACLs direkt. Waere eine Funktion SECURITY DEFINER, wuerde sie die
        // Berechtigungspruefung umgehen und der geplante Ansatz braeche.
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
            .describedAs("Rechte-Trennung per GRANT setzt SECURITY INVOKER voraus")
            .isEmpty()
    }

    @Test
    fun `metrics_result exposes queue_visible_length`() {
        // Erst ab 1.10 vorhanden und die praktisch relevantere Kennzahl fuer Rueckstands-Alarme.
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
        // Der Grund, warum die Bibliothek nur die Laenge vorpruefen und sonst nichts ablehnen sollte.
        PgmqTestDatabase.dataSource.connection.use { conn ->
            fun validate(name: String) = runCatching {
                conn.prepareStatement("SELECT pgmq.validate_queue_name(?)").use { ps ->
                    ps.setString(1, name)
                    ps.execute()
                }
            }

            assertThat(validate("a".repeat(47)).isSuccess).isTrue()
            assertThat(validate("a".repeat(48)).isSuccess).isFalse()
            // Bewusst zugelassen von pgmq — unsere Validierung darf das nicht strenger machen.
            assertThat(validate("With-Dash.And.Dots").isSuccess).isTrue()
            assertThat(validate("1leading_digit").isSuccess).isTrue()
        }
    }

    // ------------------------------------------------------------------------------------------
    // Nebenlaeufigkeit von read
    // ------------------------------------------------------------------------------------------

    @Test
    fun `concurrent readers never receive the same message`(): Unit = withQueue { queue ->
        template.sendBatch(queue, List(50) { OrderDto("N-$it", it.toLong(), emptyList()) })

        val readers = List(4) { PgmqTestDatabase.template() }
        val collected = readers.flatMap { reader ->
            reader.read<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 20).map { it.msgId }
        }

        assertThat(collected)
            .describedAs("FOR UPDATE SKIP LOCKED verhindert, dass zwei Leser dieselbe Zeile bekommen")
            .doesNotHaveDuplicates()
        assertThat(collected).hasSize(50)
    }

    // ------------------------------------------------------------------------------------------
    // FIFO-Gruppen — die Grundlage fuer `fifo = true` mit `concurrency > 1`
    // ------------------------------------------------------------------------------------------

    @Test
    fun `read_grouped skips groups that still have messages in flight`(): Unit =
        withQueue(fifo = true) { queue ->
            // Gruppe A: 4 Nachrichten, Gruppe B: 2 — verschraenkt eingestellt.
            listOf("A" to "A1", "B" to "B1", "A" to "A2", "B" to "B2", "A" to "A3", "A" to "A4")
                .forEach { (group, body) ->
                    template.send(queue, OrderDto(body, 1, emptyList()), group = group)
                }

            val first = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 2)
            assertThat(first.map { it.group }.distinct())
                .describedAs("ein read_grouped liefert Nachrichten genau einer Gruppe")
                .hasSize(1)
            val firstGroup = first.first().group

            val second = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 2)
            assertThat(second.map { it.group }.distinct()).hasSize(1)
            val secondGroup = second.first().group

            assertThat(secondGroup)
                .describedAs(
                    "Die erste Gruppe hat noch unbestaetigte Nachrichten und wird uebersprungen — " +
                        "genau deshalb bleibt die Reihenfolge innerhalb einer Gruppe auch bei " +
                        "mehreren Workern erhalten",
                )
                .isNotEqualTo(firstGroup)

            // Beide Gruppen sind jetzt in Bearbeitung: es darf nichts mehr herausgegeben werden,
            // obwohl A3 und A4 unberuehrt in der Queue liegen.
            val third = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 5)
            assertThat(third)
                .describedAs("A3/A4 sind sichtbar, duerfen aber nicht vor A1/A2 ausgeliefert werden")
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
                template.delete(queue, message.msgId) // Ack, damit die naechste freigegeben wird
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
                "Round-Robin greift quer ueber die Gruppen — deshalb bleibt fuer weitere Worker " +
                    "nichts uebrig und dieser Modus passt zu concurrency = 1",
            )
            .hasSizeGreaterThan(1)
    }

    @Test
    fun `read_grouped inside an open transaction starves other workers`(): Unit =
        withQueue(fifo = true) { queue ->
            template.send(queue, OrderDto("A1", 1, emptyList()), group = "A")
            template.send(queue, OrderDto("B1", 1, emptyList()), group = "B")

            // Der Grund, warum FIFO-Reads eine eigene kurze Transaktion brauchen: read_grouped haelt
            // einen queue-weiten Lock bis zum Transaktionsende.
            PgmqTestDatabase.dataSource.connection.use { held ->
                held.autoCommit = false
                val inTransaction = template.withConnection(held)
                    .readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 1)
                assertThat(inTransaction).hasSize(1)

                val other = template.readGrouped<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 1)
                assertThat(other)
                    .describedAs(
                        "Gruppe B waere frei, der queue-weite Lock der offenen Transaktion " +
                            "verhindert aber jeden weiteren Read",
                    )
                    .isEmpty()

                held.rollback()
            }
        }

    // ------------------------------------------------------------------------------------------
    // Header
    // ------------------------------------------------------------------------------------------

    @Test
    fun `pgmq groups by the x-pgmq-group header`(): Unit = withQueue(fifo = true) { queue ->
        template.send(queue, OrderDto("A1", 1, emptyList()), group = "gruppe-1")

        val message = template.readGrouped<OrderDto>(queue, quantity = 1).single()
        assertThat(message.group).isEqualTo("gruppe-1")
        // Die Gruppe ist ein reservierter Header und taucht nicht unter den Nutzer-Headern auf.
        assertThat(message.headers).doesNotContainKey(PgmqHeaderNames.GROUP)
    }
}
