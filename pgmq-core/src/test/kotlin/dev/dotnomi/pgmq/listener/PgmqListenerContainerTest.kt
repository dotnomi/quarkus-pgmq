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

    /** Starts a container on a fresh queue and reliably tears it down afterwards. */
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

    // --- Basic behaviour ---

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
            // Confirms the queue is empty, not merely read. Every other wait in this suite allows
            // 10-30s; there is no reason for this one to be the tightest, and a generous timeout
            // costs nothing when the test passes.
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(queue).length).isZero()
            }
        }
    }

    @Test
    fun `idle container waits the poll interval instead of spinning`() {
        withContainer(
            spec = { ListenerSpec(queue = it, pollInterval = 1.seconds) },
            handlers = listOf(pgmqPayloadHandler<OrderDto> { }),
        ) { _, container ->
            Thread.sleep(2_500)

            val info = container.info()
            assertThat(info.state).isEqualTo(ListenerState.RUNNING)
            assertThat(info.stats.processed).isZero()

            // The poll count is what separates the two cases. A spinning container would also show
            // processed=0 and a set lastPollAt, so neither of those proves anything on its own —
            // only the number of read attempts does. At a 1s interval, 2.5s allows a handful; a busy
            // loop would run into the thousands.
            assertThat(info.stats.polls)
                .describedAs("read attempts in 2.5s at a 1s poll interval")
                .isBetween(2L, 8L)
        }
    }

    @Test
    fun `a large backlog is drained without idle pauses`() {
        val received = ConcurrentLinkedQueue<Long>()
        withContainer(
            // A deliberately long pollInterval: if the container waited after every batch, this
            // test would never finish.
            spec = { ListenerSpec(queue = it, pollInterval = 30.seconds, batchSize = 10) },
            handlers = listOf(pgmqHandler<OrderDto> { received += it.msgId }),
            // Fill first, then start. Otherwise the first empty poll sends the container into
            // its 30s idle pause and the test would measure that instead of draining.
            autoStart = false,
        ) { queue, container ->
            template.createQueueIfMissing(queue)
            template.sendBatch(queue, List(100) { OrderDto("B-$it", it.toLong(), emptyList()) })
            container.start()

            await()
                .alias("100 messages must run through in one go, not in 30s steps")
                .atMost(20.seconds.toJavaDuration())
                .untilAsserted { assertThat(received).hasSize(100) }
        }
    }

    // --- Throttling ---

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
                    // Uneven processing time: the pacer has to regulate the spacing between
                    // starts, not simply wait a fixed amount afterwards.
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

            // This test measures inside the handler, not at the pacer. Between the pacer releasing a
            // slot and the timestamp being taken lie deserialization and handler lookup, and that
            // delay differs per message — it is largest for the first one, whose code paths are all
            // cold. The total therefore comes out as (n-1) x interval + (delay_last - delay_first),
            // which can land just below the exact figure. MessagePacerTest pins the guarantee where
            // it holds without tolerance; what is checked here is only that pacing is wired in.
            assertThat(totalMs)
                .describedAs("total for %d messages, gaps: %s", starts.size, gapsMs)
                .isGreaterThanOrEqualTo((starts.size - 1) * 200L - DISPATCH_JITTER_ALLOWANCE_MS)

            // A broken throttle produces gaps near zero, so half the interval separates "jittery"
            // from "not throttling at all" without being sensitive to load on the build machine.
            assertThat(gapsMs)
                .describedAs("gaps between processing starts: %s", gapsMs)
                .allSatisfy { assertThat(it).isGreaterThanOrEqualTo(100L) }
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
                    // Processing outlasts the interval — exactly the case where a per-worker
                    // pacer would quadruple the rate.
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
            // Shared pacer: (12 - 1) * 200ms = 2200ms at minimum. One pacer per worker would
            // bring that down to roughly 550ms.
            val minimumExpectedMs = (messages - 1) * interval.inWholeMilliseconds

            assertThat(elapsedMs)
                .describedAs(
                    "total for %d messages at messageInterval=%s and concurrency=%d — one " +
                        "pacer per worker would quadruple the rate",
                    messages, interval, concurrency,
                )
                .isGreaterThanOrEqualTo((minimumExpectedMs * 0.9).toLong())
        }
    }

    // --- Visibility timeout ---

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

    // --- Label dispatch ---

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

            // The livelock regression test: no message stays stuck until its visibility timeout
            // expires. This needs its own await — the handler records its payload before the
            // container acknowledges, so the queue is briefly non-empty after the block above passes.
            await().atMost(10.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(queue).length)
                    .describedAs(
                        "one container per queue with in-process dispatch — a separate consumer per " +
                            "handler would read the other one's messages, discard them and block them",
                    )
                    .isZero()
            }
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
            template.send(queue, OrderDto("G-1", 1, emptyList()), label = "Completely-Unknown")

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            val dead = template.read<OrderDto>(dlq, quantity = 1).single()
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_REASON]).contains("Completely-Unknown")
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_ORIGIN_QUEUE]).isEqualTo(queue)
            // The envelope survives, so a replay can still be deduplicated.
            assertThat(dead.envelope?.messageId).isNotBlank()
        }
    }

    // --- Kinds of failure ---

    @Test
    fun `transient failure is retried and only then dead lettered`() {
        val attempts = AtomicInteger()
        withContainer(
            spec = {
                ListenerSpec(
                    queue = it,
                    pollInterval = 100.milliseconds,
                    maxRetries = 2,
                    // Keep the backoff small so the test does not run for minutes.
                    maxRetryBackoff = 1.seconds,
                    visibilityTimeout = 60.seconds,
                )
            },
            handlers = listOf(
                pgmqPayloadHandler<OrderDto> {
                    attempts.incrementAndGet()
                    error("Temporarily unreachable")
                },
            ),
        ) { queue, container ->
            template.send(queue, OrderDto("H-1", 1, emptyList()))

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(30.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            assertThat(attempts.get())
                .describedAs("maxRetries=2 means three deliveries before the DLQ")
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
                    throw InvalidRecipientException("Recipient does not exist")
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
                    "a permanent failure must not burn backoff rounds — @PgmqNonRetryable " +
                        "means straight to the DLQ on the first attempt",
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
            // Structurally incompatible with OrderDto.
            template.send(queue, mapOf("totally" to "different", "no" to "orderId"))

            val dlq = container.spec.effectiveDeadLetterQueue
            await().atMost(15.seconds.toJavaDuration()).untilAsserted {
                assertThat(template.metrics(dlq).length).isEqualTo(1)
            }

            val dead = template.readRaw(dlq, quantity = 1).single()
            assertThat(dead.diagnostics[PgmqHeaderNames.DLQ_REASON]).containsIgnoringCase("deserial")
            // The raw payload is untouched — preserving it is the whole point.
            assertThat(dead.payload).contains("totally")
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

    // --- Envelope validation ---

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

    /** Simulates a foreign client or psql: a message without our envelope. */
    private fun sendWithoutEnvelope(queue: String, payloadJson: String) {
        PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT pgmq.send(?, ?::jsonb)").use { ps ->
                ps.setString(1, queue)
                ps.setString(2, payloadJson)
                ps.execute()
            }
        }
    }

    // --- Lifecycle ---

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
            assertThat(received).describedAs("nothing is read without start()").isEmpty()
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
                .describedAs("stop() leaves the messages in place instead of pushing them to the DLQ")
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

    // --- Graceful Shutdown ---

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
                    // Long enough that waiting for the timeout to expire would be obvious here.
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

            // Wait until the batch has been read and processing is under way.
            await().atMost(10.seconds.toJavaDuration()).until { started.isNotEmpty() }

            container.stop()

            val processedCount = started.size
            assertThat(processedCount)
                .describedAs("not all 10 should be finished before the stop")
                .isLessThan(10)

            // The actual assertion: the remaining messages are available again immediately.
            await()
                .alias("the rest of the batch must reappear without waiting out the 300s timeout")
                .atMost(10.seconds.toJavaDuration())
                .untilAsserted {
                    assertThat(template.metrics(queue).visibleLength).isGreaterThan(0)
                }
        }
    }

    @PgmqNonRetryable
    private class InvalidRecipientException(message: String) : RuntimeException(message)

    private companion object {
        /**
         * Covers the difference in dispatch delay between the first and the last message of a run.
         * Generous on purpose — a real throttling defect is off by whole intervals, not by tens of
         * milliseconds, so the tolerance costs no detection power.
         */
        const val DISPATCH_JITTER_ALLOWANCE_MS = 100L
    }
}
