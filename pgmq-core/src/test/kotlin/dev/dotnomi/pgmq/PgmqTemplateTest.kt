package dev.dotnomi.pgmq

import dev.dotnomi.pgmq.envelope.PgmqEnvelope
import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import dev.dotnomi.pgmq.support.PgmqTestDatabase.withQueue
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.time.Duration.Companion.seconds

class PgmqTemplateTest {
    private val template = PgmqTestDatabase.template()

    // ------------------------------------------------------------------------------------------
    // Queue-Verwaltung
    // ------------------------------------------------------------------------------------------

    @Test
    fun `create list exists and drop queue`() {
        val queue = PgmqTestDatabase.uniqueQueueName()
        assertThat(template.queueExists(queue)).isFalse()

        template.createQueue(queue)
        try {
            assertThat(template.queueExists(queue)).isTrue()
            assertThat(template.listQueues().map { it.name }).contains(queue)

            val info = template.listQueues().first { it.name == queue }
            assertThat(info.isPartitioned).isFalse()
            assertThat(info.isUnlogged).isFalse()
            assertThat(info.createdAt).isBefore(Instant.now().plusSeconds(5))
        } finally {
            assertThat(template.dropQueue(queue)).isTrue()
        }
        assertThat(template.queueExists(queue)).isFalse()
    }

    @Test
    fun `createQueueIfMissing is idempotent`(): Unit = withQueue { queue ->
        template.createQueueIfMissing(queue)
        template.createQueueIfMissing(queue)
        assertThat(template.queueExists(queue)).isTrue()
    }

    @Test
    fun `unlogged queue is reported as unlogged`() {
        val queue = PgmqTestDatabase.uniqueQueueName("unlogged")
        template.createUnloggedQueue(queue)
        try {
            assertThat(template.listQueues().first { it.name == queue }.isUnlogged).isTrue()
        } finally {
            template.dropQueue(queue)
        }
    }

    @Test
    fun `queue name longer than 47 characters is rejected before the roundtrip`() {
        assertThatThrownBy { template.createQueue("q".repeat(48)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("at most 47")
    }

    // ------------------------------------------------------------------------------------------
    // Senden und Lesen
    // ------------------------------------------------------------------------------------------

    @Test
    fun `send and read round trip preserves typed payload`(): Unit = withQueue { queue ->
        val order = OrderDto("A-1", 4999, listOf("Schraube", "Mutter"), note = null)
        val msgId = template.send(queue, order, label = "OrderCreated")
        assertThat(msgId).isPositive()

        val messages = template.read<OrderDto>(queue, quantity = 10)
        assertThat(messages).hasSize(1)

        val message = messages.single()
        assertThat(message.msgId).isEqualTo(msgId)
        assertThat(message.payload).isEqualTo(order)
        assertThat(message.readCount).isEqualTo(1)
        assertThat(message.label).isEqualTo("OrderCreated")
    }

    @Test
    fun `envelope round trips all eight fields`(): Unit = withQueue { queue ->
        val before = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        template.send(
            queue = queue,
            payload = OrderDto("A-2", 1, emptyList()),
            label = "OrderCreated",
            targetId = "billing",
            sourceId = "order-service",
            schemaVersion = 3,
        )

        val envelope = template.read<OrderDto>(queue).single().envelope
        assertThat(envelope).isNotNull
        requireNotNull(envelope)

        assertThat(envelope.messageId).isNotBlank()
        assertThat(envelope.sourceId).isEqualTo("order-service")
        assertThat(envelope.targetId).isEqualTo("billing")
        assertThat(envelope.label).isEqualTo("OrderCreated")
        assertThat(envelope.correlationId).isNotBlank()
        assertThat(envelope.causationId).isNull() // Kettenanfang
        assertThat(envelope.schemaVersion).isEqualTo(3)
        assertThat(envelope.sendingTime).isAfterOrEqualTo(before)
    }

    @Test
    fun `messageId is unique per message even within a batch`(): Unit = withQueue { queue ->
        template.sendBatch(queue, List(20) { OrderDto("B-$it", it.toLong(), emptyList()) })

        val ids = template.read<OrderDto>(queue, quantity = 20).mapNotNull { it.envelope?.messageId }
        assertThat(ids).hasSize(20)
        assertThat(ids.distinct()).hasSize(20)
    }

    @Test
    fun `label is optional`(): Unit = withQueue { queue ->
        template.send(queue, OrderDto("A-3", 1, emptyList()))
        val message = template.read<OrderDto>(queue).single()
        assertThat(message.label).isNull()
        assertThat(message.envelope).isNotNull
    }

    @Test
    fun `user headers survive the round trip and stay separate from the envelope`(): Unit = withQueue { queue ->
        template.send(
            queue = queue,
            payload = OrderDto("A-4", 1, emptyList()),
            label = "OrderCreated",
            headers = mapOf("priority" to "high", "tenant" to "acme"),
        )

        val message = template.read<OrderDto>(queue).single()
        assertThat(message.headers).containsExactlyInAnyOrderEntriesOf(
            mapOf("priority" to "high", "tenant" to "acme"),
        )
        // Envelope-Felder tauchen NICHT unter den Nutzer-Headern auf.
        assertThat(message.headers.keys).doesNotContain("messageId", "label", "sourceId")
    }

    @Test
    fun `user header may not occupy a reserved name`(): Unit = withQueue { queue ->
        assertThatThrownBy {
            template.send(queue, OrderDto("A-5", 1, emptyList()), headers = mapOf("label" to "gekapert"))
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("is reserved")
    }

    @Test
    fun `send batch returns one id per payload`(): Unit = withQueue { queue ->
        val payloads = List(5) { OrderDto("C-$it", it.toLong(), listOf("x")) }
        val ids = template.sendBatch(queue, payloads, label = "Bulk")

        assertThat(ids).hasSize(5)
        assertThat(ids.distinct()).hasSize(5)

        val read = template.read<OrderDto>(queue, quantity = 10)
        assertThat(read.map { it.payload }).containsExactlyInAnyOrderElementsOf(payloads)
        assertThat(read).allSatisfy { assertThat(it.label).isEqualTo("Bulk") }
    }

    @Test
    fun `delayed message is not visible before the delay elapses`(): Unit = withQueue { queue ->
        template.send(queue, OrderDto("D-1", 1, emptyList()), delay = 60.seconds)
        assertThat(template.read<OrderDto>(queue, quantity = 10)).isEmpty()
        assertThat(template.metrics(queue).length).isEqualTo(1)
        assertThat(template.metrics(queue).visibleLength).isZero()
    }

    @Test
    fun `sendAt hides the message until the given instant`(): Unit = withQueue { queue ->
        template.sendAt(queue, OrderDto("D-2", 1, emptyList()), visibleAt = Instant.now().plusSeconds(60))
        assertThat(template.read<OrderDto>(queue, quantity = 10)).isEmpty()
        assertThat(template.metrics(queue).length).isEqualTo(1)
    }

    // ------------------------------------------------------------------------------------------
    // Visibility-Timeout und Bestaetigen
    // ------------------------------------------------------------------------------------------

    @Test
    fun `read hides the message for the visibility timeout`(): Unit = withQueue { queue ->
        template.send(queue, OrderDto("E-1", 1, emptyList()))

        assertThat(template.read<OrderDto>(queue, visibilityTimeout = 60.seconds)).hasSize(1)
        // Zweiter Lesevorgang sieht nichts: die Nachricht ist noch in Bearbeitung.
        assertThat(template.read<OrderDto>(queue, visibilityTimeout = 60.seconds)).isEmpty()
    }

    @Test
    fun `setVisibilityTimeout to zero makes the message immediately visible again`(): Unit = withQueue { queue ->
        val msgId = template.send(queue, OrderDto("E-2", 1, emptyList()))
        template.read<OrderDto>(queue, visibilityTimeout = 300.seconds)
        assertThat(template.read<OrderDto>(queue)).isEmpty()

        // Der Mechanismus hinter Graceful Shutdown: sofort freigeben statt auf vt-Ablauf zu warten.
        template.setVisibilityTimeout(queue, msgId, kotlin.time.Duration.ZERO)

        val again = template.read<OrderDto>(queue)
        assertThat(again).hasSize(1)
        assertThat(again.single().readCount).isEqualTo(2)
    }

    @Test
    fun `delete removes the message permanently`(): Unit = withQueue { queue ->
        val msgId = template.send(queue, OrderDto("E-3", 1, emptyList()))
        assertThat(template.delete(queue, msgId)).isTrue()
        template.setVisibilityTimeout(queue, msgId, kotlin.time.Duration.ZERO)
        assertThat(template.read<OrderDto>(queue, quantity = 10)).isEmpty()
        assertThat(template.metrics(queue).length).isZero()
    }

    @Test
    fun `delete accepts a batch of ids`(): Unit = withQueue { queue ->
        val ids = template.sendBatch(queue, List(4) { OrderDto("E4-$it", 1, emptyList()) })
        assertThat(template.delete(queue, ids)).containsExactlyInAnyOrderElementsOf(ids)
        assertThat(template.metrics(queue).length).isZero()
    }

    @Test
    fun `archive moves the message out of the queue`(): Unit = withQueue { queue ->
        val msgId = template.send(queue, OrderDto("E-5", 1, emptyList()))
        assertThat(template.archive(queue, msgId)).isTrue()
        assertThat(template.metrics(queue).length).isZero()
    }

    @Test
    fun `pop reads and deletes in one step`(): Unit = withQueue { queue ->
        template.send(queue, OrderDto("E-6", 1, emptyList()))
        val popped = template.pop<OrderDto>(queue)
        assertThat(popped).hasSize(1)
        assertThat(template.metrics(queue).length).isZero()
    }

    @Test
    fun `purge empties the queue but keeps it`(): Unit = withQueue { queue ->
        template.sendBatch(queue, List(7) { OrderDto("F-$it", 1, emptyList()) })
        assertThat(template.purgeQueue(queue)).isEqualTo(7)
        assertThat(template.queueExists(queue)).isTrue()
        assertThat(template.metrics(queue).length).isZero()
    }

    // ------------------------------------------------------------------------------------------
    // Kennzahlen
    // ------------------------------------------------------------------------------------------

    @Test
    fun `metrics distinguishes total length from visible length`(): Unit = withQueue { queue ->
        template.sendBatch(queue, List(5) { OrderDto("G-$it", 1, emptyList()) })
        template.read<OrderDto>(queue, visibilityTimeout = 300.seconds, quantity = 2)

        val metrics = template.metrics(queue)
        assertThat(metrics.name).isEqualTo(queue)
        assertThat(metrics.length).isEqualTo(5)
        // Zwei Nachrichten sind in Bearbeitung, also nicht abholbar.
        assertThat(metrics.visibleLength).isEqualTo(3)
        assertThat(metrics.totalMessages).isGreaterThanOrEqualTo(5)
    }

    @Test
    fun `metricsAll contains the queue`(): Unit = withQueue { queue ->
        template.send(queue, OrderDto("H-1", 1, emptyList()))
        assertThat(template.metricsAll().map { it.name }).contains(queue)
    }

    // ------------------------------------------------------------------------------------------
    // Transaktionen
    // ------------------------------------------------------------------------------------------

    @Test
    fun `rolled back transaction leaves no message in the queue`(): Unit = withQueue { queue ->
        assertThatThrownBy {
            template.inTransaction { tx ->
                tx.send(queue, OrderDto("I-1", 1, emptyList()))
                tx.send(queue, OrderDto("I-2", 1, emptyList()))
                error("Fachlicher Fehler nach dem Senden")
            }
        }.hasMessageContaining("Fachlicher Fehler")

        assertThat(template.metrics(queue).length).isZero()
    }

    @Test
    fun `committed transaction publishes all messages`(): Unit = withQueue { queue ->
        val ids = template.inTransaction { tx ->
            listOf(
                tx.send(queue, OrderDto("I-3", 1, emptyList())),
                tx.send(queue, OrderDto("I-4", 1, emptyList())),
            )
        }
        assertThat(ids).hasSize(2)
        assertThat(template.metrics(queue).length).isEqualTo(2)
    }

    @Test
    fun `withConnection joins an existing transaction`(): Unit = withQueue { queue ->
        PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.autoCommit = false
            template.withConnection(conn).send(queue, OrderDto("I-5", 1, emptyList()))
            conn.rollback()
        }
        assertThat(template.metrics(queue).length).isZero()
    }

    // ------------------------------------------------------------------------------------------
    // Korrelation
    // ------------------------------------------------------------------------------------------

    @Test
    fun `send inside a handler context inherits correlation and sets causation`(): Unit = withQueue { queue ->
        val incoming = PgmqEnvelope.create(sourceId = "upstream", targetId = "me", label = "Trigger")

        PgmqExchangeContext.with(incoming) {
            template.send(queue, OrderDto("J-1", 1, emptyList()), label = "Followup")
        }

        val envelope = template.read<OrderDto>(queue).single().envelope!!
        assertThat(envelope.correlationId).isEqualTo(incoming.correlationId)
        assertThat(envelope.causationId).isEqualTo(incoming.messageId)
        assertThat(envelope.messageId).isNotEqualTo(incoming.messageId)
    }

    @Test
    fun `CorrelationPolicy NEW starts a fresh flow instead of inheriting`(): Unit = withQueue { queue ->
        val incoming = PgmqEnvelope.create(sourceId = "upstream", targetId = "me")

        PgmqExchangeContext.with(incoming) {
            template.send(
                queue = queue,
                payload = OrderDto("J-2", 1, emptyList()),
                correlation = CorrelationPolicy.NEW,
            )
        }

        val envelope = template.read<OrderDto>(queue).single().envelope!!
        assertThat(envelope.correlationId).isNotEqualTo(incoming.correlationId)
        assertThat(envelope.causationId).isNull()
    }

    @Test
    fun `causation chain survives three hops`(): Unit = withQueue { queue ->
        // Hop 1: ausserhalb jedes Kontexts
        template.send(queue, OrderDto("K-1", 1, emptyList()), label = "Hop1")
        val hop1 = template.read<OrderDto>(queue).single().envelope!!

        // Hop 2: im Kontext von Hop 1
        PgmqExchangeContext.with(hop1) { template.send(queue, OrderDto("K-2", 1, emptyList()), label = "Hop2") }
        val hop2 = template.read<OrderDto>(queue).single().envelope!!

        // Hop 3: im Kontext von Hop 2
        PgmqExchangeContext.with(hop2) { template.send(queue, OrderDto("K-3", 1, emptyList()), label = "Hop3") }
        val hop3 = template.read<OrderDto>(queue).single().envelope!!

        assertThat(listOf(hop1, hop2, hop3).map { it.correlationId }.distinct())
            .describedAs("correlationId bleibt ueber den gesamten Ablauf gleich")
            .hasSize(1)

        assertThat(hop2.causationId).isEqualTo(hop1.messageId)
        assertThat(hop3.causationId).isEqualTo(hop2.messageId)
        assertThat(hop1.causationId).isNull()
    }

    @Test
    fun `context is restored after nested use`() {
        val outer = PgmqEnvelope.create(sourceId = "a", targetId = "b")
        val inner = PgmqEnvelope.create(sourceId = "c", targetId = "d")

        assertThat(PgmqExchangeContext.current()).isNull()
        PgmqExchangeContext.with(outer) {
            assertThat(PgmqExchangeContext.current()).isEqualTo(outer)
            PgmqExchangeContext.with(inner) {
                assertThat(PgmqExchangeContext.current()).isEqualTo(inner)
            }
            assertThat(PgmqExchangeContext.current()).isEqualTo(outer)
        }
        assertThat(PgmqExchangeContext.current()).isNull()
    }

    // ------------------------------------------------------------------------------------------
    // Sonstiges
    // ------------------------------------------------------------------------------------------

    @Test
    fun `extension version is readable`() {
        assertThat(template.extensionVersion()).isNotNull().matches("""\d+\.\d+.*""")
    }

    @Test
    fun `conditional filters on the message body`(): Unit = withQueue { queue ->
        template.send(queue, mapOf("type" to "A", "n" to 1))
        template.send(queue, mapOf("type" to "B", "n" to 2))

        val onlyA = template.read<Map<String, Any>>(
            queue = queue,
            quantity = 10,
            conditional = """{"type":"A"}""",
        )
        assertThat(onlyA).hasSize(1)
        assertThat(onlyA.single().payload["type"]).isEqualTo("A")
    }

    @Test
    fun `conditional cannot filter on envelope fields because they live in headers`(): Unit = withQueue { queue ->
        template.send(queue, mapOf("n" to 1), label = "Wanted")
        template.send(queue, mapOf("n" to 2), label = "Unwanted")

        // Dokumentiert die verifizierte Einschraenkung: der conditional-Filter matcht nur auf die
        // message-Spalte. Ein Filter auf ein Envelope-Feld findet daher nichts.
        val byLabel = template.read<Map<String, Any>>(
            queue = queue,
            quantity = 10,
            conditional = """{"label":"Wanted"}""",
        )
        assertThat(byLabel)
            .describedAs("Envelope-Felder sind nicht serverseitig filterbar — daher Label-Dispatch in-process")
            .isEmpty()
    }
}
