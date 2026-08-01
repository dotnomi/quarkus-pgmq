package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.envelope.EnvelopeValidation
import dev.dotnomi.pgmq.envelope.PgmqHeaderNames
import dev.dotnomi.pgmq.read
import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class PgmqListenerContainerTest {
    private val template: PgmqTemplate = PgmqTestDatabase.template("listener-test")

    /** Startet einen Container fuer eine frische Queue und raeumt ihn danach zuverlaessig ab. */
    private fun <R> withContainer(
        spec: (queue: String) -> ListenerSpec,
        handlers: List<RegisteredHandler<*>>,
        autoStart: Boolean = true,
        block: (queue: String, container: PgmqListenerContainer) -> R,
    ): R {
        val queue = PgmqTestDatabase.uniqueQueueName("lsn")
        val resolved = spec(queue)
        val container = PgmqListenerContainer(resolved, template, handlers)
        return try {
            if (autoStart) container.start()
            block(queue, container)
        } finally {
            runCatching { container.stop() }
            runCatching { template.dropQueue(queue) }
            runCatching { template.dropQueue(resolved.effectiveDeadLetterQueue) }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Grundverhalten
    // ------------------------------------------------------------------------------------------

    @Test
    fun `messages are consumed and acknowledged`() {
        val received = ConcurrentLinkedQueue<OrderDto>()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { received += it }),
        ) { queue, _ ->
            template.sendBatch(queue, List(5) { OrderDto("A-$it", it.toLong(), emptyList()) })

            await().atMost(10.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).hasSize(5)
            }
            // Bestaetigt: die Queue ist leer, nicht nur gelesen.
            await().atMost(5.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(queue).length).isZero()
            }
        }
    }

    @Test
    fun `idle container waits the poll interval instead of spinning`() {
        val polls = AtomicInteger()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 1.seconds) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { }),
        ) { _, container ->
            // Ueber 2,5s duerfen bei 1s Intervall nur wenige Polls stattfinden.
            Thread.sleep(2_500)
            val info = container.info()
            assertThat(info.state).isEqualTo(ListenerState.RUNNING)
            assertThat(info.stats.processed).isZero()
            // lastPollAt wird bei jedem Poll gesetzt; dass es gesetzt ist, zeigt dass gepollt wurde.
            assertThat(info.stats.lastPollAt).isNotNull()
            polls.get() // nur zur Vollstaendigkeit
        }
    }

    @Test
    fun `a large backlog is drained without idle pauses`() {
        val received = ConcurrentLinkedQueue<Long>()
        withContainer(
            // pollInterval bewusst hoch: wuerde nach jedem Batch gewartet, kaeme der Test nie durch.
            spec = { ListenerSpec(queue = it, pollInterval = 30.seconds, batchSize = 10) },
            handlers = listOf(pgmqHandler<OrderDto> { received += it.msgId }),
            // Erst fuellen, dann starten. Sonst geht der Container beim ersten leeren Poll in die
            // 30s-Leerlaufpause und der Test wuerde nur diese messen statt das Drain-Verhalten.
            autoStart = false,
        ) { queue, container ->
            template.createQueueIfMissing(queue)
            template.sendBatch(queue, List(100) { OrderDto("B-$it", it.toLong(), emptyList()) })
            container.start()

            await()
                .alias("100 Nachrichten muessen am Stueck durchlaufen, nicht in 30s-Schritten")
                .atMost(20.seconds.toJavaDuration())
                .untilAsserted { assertThat(received).hasSize(100) }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Drosselung
    // ------------------------------------------------------------------------------------------

    @Test
    fun `messageInterval spaces out processing starts`() {
        val timestamps = ConcurrentLinkedQueue<Long>()
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    messageInterval = 200.milliseconds,
                    batchSize = 5,
                    concurrency = 1,
                )
            },
            handlers = listOf(
                pgmqHandler<OrderDto> {
                    timestamps += System.nanoTime()
                    // Ungleichmaessige Verarbeitungsdauer: der Pacer muss auf den Abstand zwischen
                    // Starts regeln, nicht pauschal nachher warten.
                    Thread.sleep(if (timestamps.size % 2 == 0) 10 else 90)
                },
            ),
        ) { queue, _ ->
            template.sendBatch(queue, List(5) { OrderDto("C-$it", it.toLong(), emptyList()) })

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(timestamps).hasSize(5)
            }

            val starts = timestamps.toList()
            val gapsMs = starts.zipWithNext { a, b -> (b - a) / 1_000_000 }
            val totalMs = (starts.last() - starts.first()) / 1_000_000

            // Die eigentliche Zusage ist die **Gesamtrate**: der Pacer reserviert exakte Zeitschlitze,
            // also braucht ein Durchlauf von n Nachrichten mindestens (n-1) x Intervall. Ohne Toleranz.
            assertThat(totalMs)
                .describedAs("Gesamtdauer fuer %d Nachrichten, Abstaende: %s", starts.size, gapsMs)
                .isGreaterThanOrEqualTo((starts.size - 1) * 200L)

            // Einzelne Abstaende schwanken um die Schlitze herum, weil zwischen Pacer-Freigabe und
            // dem Zeitstempel im Handler noch Deserialisierung und Handler-Suche liegen, deren Dauer
            // pro Nachricht variiert. Geprueft wird deshalb nur, dass keine Nachricht *deutlich* zu
            // frueh laeuft — eine echt kaputte Drosselung faellt hier auf, Dispatch-Jitter nicht.
            assertThat(gapsMs)
                .describedAs("Abstaende zwischen Verarbeitungsstarts: %s", gapsMs)
                .allSatisfy { assertThat(it).isGreaterThanOrEqualTo(140L) }
        }
    }

    @Test
    fun `pacer is shared across workers so concurrency does not multiply the rate`() {
        val timestamps = ConcurrentLinkedQueue<Long>()
        val messages = 12
        val interval = 200.milliseconds
        val concurrency = 4

        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    messageInterval = interval,
                    batchSize = 3,
                    concurrency = concurrency,
                    // Verarbeitung dauert laenger als das Intervall — genau der Fall, in dem ein
                    // Pacer pro Worker die Rate vervierfachen wuerde.
                    visibilityTimeout = 120.seconds,
                )
            },
            handlers = listOf(
                pgmqHandler<OrderDto> {
                    timestamps += System.nanoTime()
                    Thread.sleep(500)
                },
            ),
        ) { queue, _ ->
            template.sendBatch(queue, List(messages) { OrderDto("D-$it", it.toLong(), emptyList()) })

            await().atMost(30.seconds.toJavaDuration()).untilAsserted {
                assertThat(timestamps).hasSize(messages)
            }

            val all = timestamps.toList().sorted()
            val elapsedMs = (all.last() - all.first()) / 1_000_000
            // Bei geteiltem Pacer: (12 - 1) * 200ms = 2200ms Mindestdauer.
            // Bei einem Pacer pro Worker waeren es nur ~550ms.
            val minimumExpectedMs = (messages - 1) * interval.inWholeMilliseconds

            assertThat(elapsedMs)
                .describedAs(
                    "Gesamtdauer fuer %d Nachrichten bei messageInterval=%s und concurrency=%d — " +
                        "ein Pacer pro Worker wuerde die Rate vervierfachen",
                    messages, interval, concurrency,
                )
                .isGreaterThanOrEqualTo((minimumExpectedMs * 0.9).toLong())
        }
    }

    // ------------------------------------------------------------------------------------------
    // Visibility-Timeout
    // ------------------------------------------------------------------------------------------

    @Test
    fun `explicit visibility timeout shorter than the throttled batch is rejected`() {
        assertThatThrownBy {
            ListenerSpec(
                queue = "q",
                batchSize = 10,
                messageInterval = 1.seconds,
                visibilityTimeout = 5.seconds, // Batch braucht mindestens 10s
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("too short")
            .hasMessageContaining("processed twice")
    }

    @Test
    fun `visibility timeout is derived from batch size and message interval`() {
        val spec = ListenerSpec(queue = "q", batchSize = 10, messageInterval = 1.seconds)
        // 10 x 1s x Sicherheitsfaktor 3
        assertThat(spec.effectiveVisibilityTimeout).isEqualTo(30.seconds)

        val undrosselt = ListenerSpec(queue = "q", batchSize = 10)
        assertThat(undrosselt.effectiveVisibilityTimeout)
            .isEqualTo(ListenerSpec.DEFAULT_VISIBILITY_TIMEOUT)
    }

    // ------------------------------------------------------------------------------------------
    // Label-Dispatch
    // ------------------------------------------------------------------------------------------

    @Test
    fun `two labelled handlers on one queue both receive their messages`() {
        val created = ConcurrentLinkedQueue<String>()
        val cancelled = ConcurrentLinkedQueue<String>()

        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto>(label = "OrderCreated") { created += it.orderId },
                pgmqPayloadHandler<OrderDto>(label = "OrderCancelled") { cancelled += it.orderId },
            ),
        ) { queue, _ ->
            template.send(queue, OrderDto("E-1", 1, emptyList()), label = "OrderCreated")
            template.send(queue, OrderDto("E-2", 1, emptyList()), label = "OrderCancelled")
            template.send(queue, OrderDto("E-3", 1, emptyList()), label = "OrderCreated")

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(created).containsExactlyInAnyOrder("E-1", "E-3")
                assertThat(cancelled).containsExactly("E-2")
            }

            // Der Livelock-Regressionstest: keine Nachricht bleibt bis zum vt-Ablauf haengen.
            assertThat(template.metrics(queue).length)
                .describedAs(
                    "Ein Container pro Queue mit In-Process-Dispatch — je Handler ein eigener " +
                        "Consumer wuerde Nachrichten des anderen lesen, verwerfen und blockieren",
                )
                .isZero()
        }
    }

    @Test
    fun `handler without label acts as catch-all`() {
        val labelled = ConcurrentLinkedQueue<String>()
        val fallback = ConcurrentLinkedQueue<String>()

        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto>(label = "Known") { labelled += it.orderId },
                pgmqPayloadHandler<OrderDto>(name = "fallback") { fallback += it.orderId },
            ),
        ) { queue, _ ->
            template.send(queue, OrderDto("F-1", 1, emptyList()), label = "Known")
            template.send(queue, OrderDto("F-2", 1, emptyList()), label = "Unknown")
            template.send(queue, OrderDto("F-3", 1, emptyList())) // ganz ohne Label

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(labelled).containsExactly("F-1")
                assertThat(fallback).containsExactlyInAnyOrder("F-2", "F-3")
            }
        }
    }

    @Test
    fun `two catch-all handlers on one queue are rejected`() {
        assertThatThrownBy {
            PgmqListenerContainer(
                ListenerSpec(queue = "q"),
                template,
                listOf(
                    pgmqPayloadHandler<OrderDto>(name = "erster") { },
                    pgmqPayloadHandler<OrderDto>(name = "zweiter") { },
                ),
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("catch-all")
    }

    @Test
    fun `message without a matching handler goes to the dead letter queue`() {
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(pgmqPayloadHandler<OrderDto>(label = "Known") { }),
        ) { queue, container ->
            template.send(queue, OrderDto("G-1", 1, emptyList()), label = "Voellig-Unbekannt")

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            val dead = template.read<OrderDto>(dlq, quantity = 1).single()
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_REASON]).contains("Voellig-Unbekannt")
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_ORIGIN_QUEUE]).isEqualTo(queue)
            // Der Envelope bleibt erhalten, damit ein Replay dedupliziertbar ist.
            assertThat(dead.envelope?.messageId).isNotBlank()
        }
    }

    // ------------------------------------------------------------------------------------------
    // Fehlerarten
    // ------------------------------------------------------------------------------------------

    @Test
    fun `transient failure is retried and only then dead lettered`() {
        val attempts = AtomicInteger()
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    maxRetries = 2,
                    // Backoff klein halten, damit der Test nicht minutenlang laeuft.
                    maxRetryBackoff = 1.seconds,
                    visibilityTimeout = 60.seconds,
                )
            },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto> {
                    attempts.incrementAndGet()
                    error("Vorruebergehend nicht erreichbar")
                },
            ),
        ) { queue, container ->
            template.send(queue, OrderDto("H-1", 1, emptyList()))

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(30.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            assertThat(attempts.get())
                .describedAs("maxRetries=2 bedeutet 3 Zustellungen vor der DLQ")
                .isGreaterThanOrEqualTo(3)
        }
    }

    @Test
    fun `permanent failure goes to the dead letter queue on the first attempt`() {
        val attempts = AtomicInteger()
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    maxRetries = 5,
                    visibilityTimeout = 60.seconds,
                )
            },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto> {
                    attempts.incrementAndGet()
                    throw InvalidRecipientException("Empfaenger existiert nicht")
                },
            ),
        ) { queue, container ->
            template.send(queue, OrderDto("I-1", 1, emptyList()))

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            assertThat(attempts.get())
                .describedAs(
                    "Ein dauerhafter Fehler darf keine Backoff-Runden verbrennen — " +
                        "@PgmqNonRetryable heisst: beim ersten Versuch in die DLQ",
                )
                .isEqualTo(1)
        }
    }

    @Test
    fun `undeserializable payload is treated as permanent`() {
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds, maxRetries = 5) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { }),
        ) { queue, container ->
            // Passt strukturell nicht zu OrderDto.
            template.send(queue, mapOf("voellig" to "anders", "kein" to "orderId"))

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            val dead = template.readRaw(dlq, quantity = 1).single()
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_REASON]).containsIgnoringCase("deserial")
            // Der rohe Payload bleibt unveraendert — er ist ja genau deshalb hier.
            assertThat(dead.payload).contains("voellig")
        }
    }

    @Test
    fun `unsupported schema version is rejected without retry`() {
        val attempts = AtomicInteger()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds, maxRetries = 5) },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto>(schemaVersions = 1..2) { attempts.incrementAndGet() },
            ),
        ) { queue, container ->
            template.send(queue, OrderDto("J-1", 1, emptyList()), schemaVersion = 7)

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }
            assertThat(attempts.get()).isZero()

            val dead = template.readRaw(dlq, quantity = 1).single()
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_REASON]).contains("schemaVersion 7")
        }
    }

    @Test
    fun `supported schema version is processed`() {
        val received = ConcurrentLinkedQueue<String>()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto>(schemaVersions = 1..2) { received += it.orderId },
            ),
        ) { queue, _ ->
            template.send(queue, OrderDto("J-2", 1, emptyList()), schemaVersion = 2)
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("J-2")
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Envelope-Validierung
    // ------------------------------------------------------------------------------------------

    @Test
    fun `STRICT rejects a foreign message without an envelope`() {
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    envelopeValidation = EnvelopeValidation.STRICT,
                )
            },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { }),
        ) { queue, container ->
            sendWithoutEnvelope(queue, """{"orderId":"K-1","amountCents":1,"items":[]}""")

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }
        }
    }

    @Test
    fun `LENIENT processes a foreign message without an envelope`() {
        val received = ConcurrentLinkedQueue<String>()
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    envelopeValidation = EnvelopeValidation.LENIENT,
                )
            },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { received += it.orderId }),
        ) { queue, _ ->
            sendWithoutEnvelope(queue, """{"orderId":"K-2","amountCents":1,"items":[]}""")

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("K-2")
            }
        }
    }

    /** Simuliert einen Fremd-Client oder psql: Nachricht ohne unseren Envelope. */
    private fun sendWithoutEnvelope(queue: String, payloadJson: String) {
        PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT pgmq.send(?, ?::jsonb)").use { ps ->
                ps.setString(1, queue)
                ps.setString(2, payloadJson)
                ps.execute()
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Lebenszyklus
    // ------------------------------------------------------------------------------------------

    @Test
    fun `autoStart false means nothing is read until start is called`() {
        val received = ConcurrentLinkedQueue<String>()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds, autoStart = false) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { received += it.orderId }),
            autoStart = false,
        ) { queue, container ->
            template.createQueueIfMissing(queue)
            template.send(queue, OrderDto("L-1", 1, emptyList()))

            assertThat(container.state()).isEqualTo(ListenerState.NOT_STARTED)
            Thread.sleep(1_000)
            assertThat(received).describedAs("ohne start() wird nichts gelesen").isEmpty()
            assertThat(template.metrics(queue).length).isEqualTo(1)

            container.start()
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("L-1")
            }
        }
    }

    @Test
    fun `stop leaves messages in the queue and start resumes consumption`() {
        val received = ConcurrentLinkedQueue<String>()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { received += it.orderId }),
        ) { queue, container ->
            container.stop()
            assertThat(container.state()).isEqualTo(ListenerState.STOPPED)

            template.send(queue, OrderDto("M-1", 1, emptyList()))
            Thread.sleep(1_000)
            assertThat(received).isEmpty()
            assertThat(template.metrics(queue).length)
                .describedAs("stop() laesst die Nachrichten liegen, statt sie in die DLQ zu schieben")
                .isEqualTo(1)

            container.start()
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("M-1")
            }
        }
    }

    @Test
    fun `pause halts reading and resume continues`() {
        val received = ConcurrentLinkedQueue<String>()
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { received += it.orderId }),
        ) { queue, container ->
            container.pause()
            assertThat(container.state()).isEqualTo(ListenerState.PAUSED)

            template.send(queue, OrderDto("N-1", 1, emptyList()))
            Thread.sleep(1_000)
            assertThat(received).isEmpty()

            container.resume()
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(received).containsExactly("N-1")
            }
        }
    }

    @Test
    fun `info reports handlers and statistics`() {
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 100.milliseconds, concurrency = 2) },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto>(label = "A") { },
                pgmqPayloadHandler<OrderDto>(label = "B") { },
            ),
        ) { queue, container ->
            template.send(queue, OrderDto("O-1", 1, emptyList()), label = "A")

            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(container.info().stats.processed).isEqualTo(1)
            }

            val info = container.info()
            assertThat(info.id).isEqualTo(queue)
            assertThat(info.queue).isEqualTo(queue)
            assertThat(info.concurrency).isEqualTo(2)
            assertThat(info.handlers.map { it.label }).containsExactlyInAnyOrder("A", "B")
            assertThat(info.state).isEqualTo(ListenerState.RUNNING)
        }
    }

    @Test
    fun `manual ack mode leaves acknowledgement to the handler`() {
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    ackMode = AckMode.MANUAL,
                    visibilityTimeout = 120.seconds,
                )
            },
            handlers = listOf(
                pgmqHandlerWithContext<OrderDto> { _, context -> context.archive() },
            ),
        ) { queue, _ ->
            template.send(queue, OrderDto("P-1", 1, emptyList()))
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(queue).length).isZero()
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Graceful Shutdown
    // ------------------------------------------------------------------------------------------

    @Test
    fun `stop releases the rest of the batch immediately instead of waiting for the timeout`() {
        val started = ConcurrentLinkedQueue<String>()
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    batchSize = 10,
                    concurrency = 1,
                    // Lang genug, dass ein Warten auf vt-Ablauf im Test klar auffallen wuerde.
                    visibilityTimeout = 300.seconds,
                    shutdownTimeout = 5.seconds,
                )
            },
            handlers = listOf(
                pgmqHandler<OrderDto> {
                    started += it.payload.orderId
                    Thread.sleep(300)
                },
            ),
        ) { queue, container ->
            template.sendBatch(queue, List(10) { OrderDto("Q-$it", it.toLong(), emptyList()) })

            // Warten, bis der Batch gelesen ist und die Verarbeitung laeuft.
            await().atMost(10.seconds.toJavaDuration()).until { started.isNotEmpty() }

            container.stop()

            val processedCount = started.size
            assertThat(processedCount)
                .describedAs("nicht alle 10 sollten vor dem Stop fertig sein")
                .isLessThan(10)

            // Der eigentliche Test: die restlichen Nachrichten sind SOFORT wieder abholbar.
            await()
                .alias("Rest des Batches muss ohne Warten auf das 300s-vt wieder sichtbar sein")
                .atMost(10.seconds.toJavaDuration())
                .untilAsserted {
                    assertThat(template.metrics(queue).visibleLength).isGreaterThan(0)
                }
        }
    }

    @PgmqNonRetryable
    private class InvalidRecipientException(message: String) : RuntimeException(message)
}
