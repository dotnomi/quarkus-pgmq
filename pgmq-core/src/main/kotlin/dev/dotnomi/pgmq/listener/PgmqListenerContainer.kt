package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.PgmqExchangeContext
import dev.dotnomi.pgmq.PgmqMessage
import dev.dotnomi.pgmq.metrics.DeadLetterReason
import dev.dotnomi.pgmq.metrics.MessageOutcome
import dev.dotnomi.pgmq.metrics.PgmqMetrics
import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.envelope.EnvelopeValidation
import dev.dotnomi.pgmq.envelope.PgmqEnvelope
import dev.dotnomi.pgmq.envelope.PgmqEnvelopeException
import dev.dotnomi.pgmq.envelope.PgmqHeaderNames
import dev.dotnomi.pgmq.serializer.PgmqDeserializationException
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

enum class ListenerState { NOT_STARTED, RUNNING, PAUSED, STOPPING, STOPPED, FAILED }

data class ListenerStats(
    val processed: Long,
    val failed: Long,
    val deadLettered: Long,
    val inFlight: Int,
    val lastPollAt: java.time.Instant?,
    /** Completed read attempts, empty ones included. */
    val polls: Long,
)

data class HandlerInfo(val name: String, val label: String?, val batch: Boolean)

data class PgmqListenerInfo(
    val id: String,
    val queue: String,
    /** Where this listener's failed messages go. Derived from [queue] unless configured. */
    val deadLetterQueue: String,
    val client: String,
    val handlers: List<HandlerInfo>,
    val state: ListenerState,
    val autoStart: Boolean,
    val concurrency: Int,
    val stats: ListenerStats,
)

/**
 * Reads one queue and dispatches its messages in-process to the registered handlers.
 *
 * Exactly one container per queue. Separate consumers per handler are impossible: `label` lives in
 * the headers, which pgmq cannot filter on server-side, so they would read and discard each other's
 * messages.
 */
class PgmqListenerContainer(
    val spec: ListenerSpec,
    private val template: PgmqTemplate,
    handlers: List<RegisteredHandler<*>>,
    private val metrics: PgmqMetrics = PgmqMetrics.NOOP,
) {
    private val log = LoggerFactory.getLogger("dev.dotnomi.pgmq.listener.${spec.queue}")

    private val labelled: Map<String, RegisteredHandler<*>>
    private val catchAll: RegisteredHandler<*>?

    /** Whether the handlers take whole batches. Uniform across the queue. */
    private val batchMode: Boolean

    private val state = AtomicReference(ListenerState.NOT_STARTED)
    private val running = AtomicBoolean(false)
    private val paused = AtomicBoolean(false)
    private val pacer = MessagePacer(spec.messageInterval)

    private val processed = AtomicLong()
    private val failed = AtomicLong()
    private val deadLettered = AtomicLong()
    private val inFlight = AtomicInteger()
    private val lastPollAt = AtomicReference<java.time.Instant?>(null)
    private val polls = AtomicLong()

    private var executor: ExecutorService? = null
    private var workersFinished: CountDownLatch? = null

    val id: String get() = spec.id

    init {
        require(handlers.isNotEmpty()) { "Container for queue '${spec.queue}' has no handlers." }

        val catchAllHandlers = handlers.filter { it.label == null }
        require(catchAllHandlers.size <= 1) {
            "Queue '${spec.queue}' has ${catchAllHandlers.size} catch-all handlers " +
                "(${catchAllHandlers.map { it.name }}), but at most one is allowed. A handler " +
                "without a label picks up everything no labelled handler claims."
        }
        catchAll = catchAllHandlers.firstOrNull()

        val byLabel = handlers.filter { it.label != null }.groupBy { it.label!! }
        val duplicates = byLabel.filterValues { it.size > 1 }
        require(duplicates.isEmpty()) {
            "Queue '${spec.queue}' has several handlers for the same label: " +
                duplicates.map { (label, hs) -> "$label -> ${hs.map { it.name }}" }
        }
        labelled = byLabel.mapValues { it.value.single() }

        // Mixed modes would leave the unit of work undefined for pacing, vtRefresh and FIFO abort.
        val batchModes = handlers.map { it.batch }.distinct()
        require(batchModes.size == 1) {
            "Queue '${spec.queue}' mixes batch and single-message handlers " +
                "(${handlers.map { "${it.name}=batch:${it.batch}" }}). All handlers of a queue share " +
                "one processing loop, so they must agree on batch mode."
        }
        batchMode = batchModes.single()
    }

    // --- Lifecycle ---

    fun start() {
        if (!running.compareAndSet(false, true)) {
            log.debug("Container {} is already running.", id)
            return
        }

        if (spec.autoCreateQueue) {
            template.createQueueIfMissing(spec.queue)
            if (spec.readStrategy != ReadStrategy.PLAIN) template.createFifoIndex(spec.queue)
            if (spec.unroutable == UnroutableMessagePolicy.DLQ || spec.maxRetries >= 0) {
                template.createQueueIfMissing(spec.effectiveDeadLetterQueue)
            }
        }

        paused.set(false)
        state.set(ListenerState.RUNNING)

        val latch = CountDownLatch(spec.concurrency)
        workersFinished = latch

        // A dedicated named pool so a slow listener cannot starve others.
        val threadCounter = AtomicInteger()
        val pool = Executors.newFixedThreadPool(
            spec.concurrency,
            ThreadFactory { runnable ->
                Thread(runnable, "pgmq-${spec.queue}-${threadCounter.incrementAndGet()}").apply {
                    isDaemon = true
                }
            },
        )
        executor = pool

        repeat(spec.concurrency) {
            pool.submit {
                try {
                    workerLoop()
                } catch (e: Throwable) {
                    log.error("A worker of {} terminated unexpectedly.", id, e)
                    state.set(ListenerState.FAILED)
                } finally {
                    latch.countDown()
                }
            }
        }

        log.info(
            "Listener {} started: concurrency={}, batchSize={}, pollInterval={}, " +
                "messageInterval={}, vt={}, handlers={}",
            id, spec.concurrency, spec.batchSize, spec.pollInterval, spec.messageInterval,
            spec.effectiveVisibilityTimeout, handlerNames(),
        )
    }

    /**
     * Stops reading, finishes the message in flight, and releases the rest of the batch immediately
     * so the next instance can pick it up without waiting for the visibility timeout.
     */
    fun stop() {
        if (!running.compareAndSet(true, false)) return
        state.set(ListenerState.STOPPING)

        val pool = executor
        pool?.shutdownNow() // interrupts the poll wait and the pacer

        val finished = workersFinished?.await(spec.shutdownTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        if (finished == false) {
            log.warn(
                "Listener {} exceeded its shutdown timeout of {}; in-flight messages will be " +
                    "processed again elsewhere.",
                id, spec.shutdownTimeout,
            )
        }

        pool?.awaitTermination(2, TimeUnit.SECONDS)
        executor = null
        workersFinished = null
        state.set(ListenerState.STOPPED)
        log.info("Listener {} stopped.", id)
    }

    /** Stops reading but keeps the worker threads alive. */
    fun pause() {
        if (running.get() && paused.compareAndSet(false, true)) {
            state.set(ListenerState.PAUSED)
            log.info("Listener {} paused.", id)
        }
    }

    fun resume() {
        if (running.get() && paused.compareAndSet(true, false)) {
            state.set(ListenerState.RUNNING)
            log.info("Listener {} resumed.", id)
        }
    }

    fun state(): ListenerState = state.get()

    fun info(): PgmqListenerInfo = PgmqListenerInfo(
        id = id,
        queue = spec.queue,
        deadLetterQueue = spec.effectiveDeadLetterQueue,
        client = spec.client,
        handlers = (labelled.values + listOfNotNull(catchAll))
            .map { HandlerInfo(it.name, it.label, it.batch) },
        state = state.get(),
        autoStart = spec.autoStart,
        concurrency = spec.concurrency,
        stats = ListenerStats(
            processed = processed.get(),
            failed = failed.get(),
            deadLettered = deadLettered.get(),
            inFlight = inFlight.get(),
            lastPollAt = lastPollAt.get(),
            polls = polls.get(),
        ),
    )

    private fun handlerNames() = (labelled.values + listOfNotNull(catchAll)).map { it.name }

    // --- Poll loop ---

    private fun workerLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted) {
            if (paused.get()) {
                if (!sleepInterruptible(PAUSE_CHECK_INTERVAL)) return
                continue
            }

            val batch = try {
                readBatch()
            } catch (e: Throwable) {
                if (!running.get()) return
                log.error("Reading from {} failed; next attempt in {}.", spec.queue, spec.pollInterval, e)
                if (!sleepInterruptible(spec.pollInterval)) return
                continue
            }

            lastPollAt.set(java.time.Instant.now())
            polls.incrementAndGet()

            if (batch.isEmpty()) {
                // Only an idle queue waits. While messages keep coming the loop reads on without
                // pausing, so a large backlog drains in one go.
                if (!sleepInterruptible(spec.pollInterval)) return
                continue
            }

            processBatch(batch)
        }
    }

    private fun readBatch(): List<PgmqMessage<String>> = when (spec.readStrategy) {
        ReadStrategy.PLAIN -> template.readRaw(
            queue = spec.queue,
            visibilityTimeout = spec.effectiveVisibilityTimeout,
            quantity = spec.batchSize,
        )
        ReadStrategy.GROUPED -> template.readGroupedRaw(
            queue = spec.queue,
            visibilityTimeout = spec.effectiveVisibilityTimeout,
            quantity = spec.batchSize,
        )
        ReadStrategy.GROUPED_ROUND_ROBIN -> template.readGroupedRoundRobinRaw(
            queue = spec.queue,
            visibilityTimeout = spec.effectiveVisibilityTimeout,
            quantity = spec.batchSize,
        )
    }

    private fun processBatch(batch: List<PgmqMessage<String>>) {
        if (batchMode) {
            processWholeBatch(batch)
            return
        }
        processOneByOne(batch)
    }

    /**
     * Hands every message of the read to its handler in one call.
     *
     * The batch is one unit of work: the pacer ticks once and a failure retries all of it.
     */
    private fun processWholeBatch(batch: List<PgmqMessage<String>>) {
        pacer.await()
        if (!running.get()) {
            releaseImmediately(batch)
            return
        }

        // Routing first, so a bad message does not take the rest of the batch down with it.
        val routed = LinkedHashMap<RegisteredHandler<*>, MutableList<PgmqMessage<String>>>()
        batch.forEach { raw ->
            val envelope = try {
                resolveEnvelope(raw)
            } catch (e: PgmqEnvelopeException) {
                deadLetter(raw, e.message ?: "Envelope invalid", DeadLetterReason.MALFORMED)
                return@forEach
            }
            val handler = resolveHandler(envelope?.label)
            if (handler == null) {
                handleUnroutable(raw, envelope?.label)
                return@forEach
            }
            routed.computeIfAbsent(handler) { mutableListOf() } += raw
        }

        routed.forEach { (handler, messages) ->
            inFlight.addAndGet(messages.size)
            val startedAt = System.nanoTime()
            var outcome = MessageOutcome.SUCCESS
            try {
                invokeBatch(handler, messages)
                processed.addAndGet(messages.size.toLong())
            } catch (e: Throwable) {
                outcome = MessageOutcome.RETRIED
                messages.forEach { onHandlerFailure(it, e) }
            } finally {
                inFlight.addAndGet(-messages.size)
                // In batch mode the batch is the unit of work, so this is one observation for the
                // whole batch rather than one per message.
                metrics.messageProcessed(
                    queue = spec.queue,
                    label = handler.name,
                    outcome = outcome,
                    duration = (System.nanoTime() - startedAt).nanoseconds,
                )
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun invokeBatch(handler: RegisteredHandler<*>, messages: List<PgmqMessage<String>>) {
        val typed = messages.map { it.withPayload(convert(handler, it.payload)) }
        val context = ContainerContext(messages.first())

        // A batch has no single correlation; the first message's envelope is the closest thing.
        PgmqExchangeContext.with(messages.first().envelope) {
            (handler as RegisteredHandler<Any?>).handle(typed as List<PgmqMessage<Any?>>, context)
        }

        if (context.acknowledged) return
        when (spec.ackMode) {
            AckMode.DELETE -> template.delete(spec.queue, messages.map { it.msgId })
            AckMode.ARCHIVE -> template.archive(spec.queue, messages.map { it.msgId })
            AckMode.MANUAL -> Unit
        }
    }

    private fun processOneByOne(batch: List<PgmqMessage<String>>) {
        val remaining = batch.toMutableList()

        while (remaining.isNotEmpty()) {
            if (!running.get() || Thread.currentThread().isInterrupted) {
                // Shutting down: release the rest at once so the next pod picks it up instead of
                // waiting out the visibility timeout.
                releaseImmediately(remaining)
                return
            }

            val message = remaining.removeFirst()

            pacer.await()
            if (!running.get()) {
                releaseImmediately(listOf(message) + remaining)
                return
            }

            if (spec.vtRefresh && remaining.isNotEmpty()) {
                // Extend the timeout of the messages still queued up, so a long batch cannot let
                // them fall due while they wait their turn.
                runCatching {
                    template.setVisibilityTimeout(
                        spec.queue,
                        remaining.map { it.msgId },
                        spec.effectiveVisibilityTimeout,
                    )
                }.onFailure { log.warn("vtRefresh for {} failed.", spec.queue, it) }
            }

            val outcome = processOne(message)

            // In FIFO mode nothing else of the same group may run after a failure, or the
            // ordering guarantee is broken.
            if (outcome == Outcome.FAILED_RETRYABLE && spec.readStrategy != ReadStrategy.PLAIN) {
                if (remaining.isNotEmpty()) {
                    log.warn(
                        "FIFO: message {} failed, releasing the remaining {} messages of its " +
                            "group instead of running ahead of it.",
                        message.msgId, remaining.size,
                    )
                    releaseImmediately(remaining)
                }
                return
            }
        }
    }

    private enum class Outcome(val reported: MessageOutcome) {
        OK(MessageOutcome.SUCCESS),
        FAILED_RETRYABLE(MessageOutcome.RETRIED),
        DEAD_LETTERED(MessageOutcome.DEAD_LETTERED),
        UNROUTABLE(MessageOutcome.UNROUTABLE),
    }

    private fun processOne(raw: PgmqMessage<String>): Outcome {
        inFlight.incrementAndGet()
        val startedAt = System.nanoTime()
        // Only a resolved handler's name may become a metric tag; a message label is free text.
        var handlerLabel = UNROUTED_LABEL

        val outcome = try {
            dispatch(raw) { handlerLabel = it }
        } finally {
            inFlight.decrementAndGet()
        }

        metrics.messageProcessed(
            queue = spec.queue,
            label = handlerLabel,
            outcome = outcome.reported,
            duration = (System.nanoTime() - startedAt).nanoseconds,
        )
        return outcome
    }

    private inline fun dispatch(raw: PgmqMessage<String>, onHandlerResolved: (String) -> Unit): Outcome {
        val envelope = try {
            resolveEnvelope(raw)
        } catch (e: PgmqEnvelopeException) {
            log.warn("Envelope of msg_id={} in {} is unusable: {}", raw.msgId, spec.queue, e.message)
            deadLetter(raw, e.message ?: "Envelope invalid", DeadLetterReason.MALFORMED)
            return Outcome.DEAD_LETTERED
        }

        val handler = resolveHandler(envelope?.label)
        if (handler == null) {
            handleUnroutable(raw, envelope?.label)
            return Outcome.UNROUTABLE
        }
        onHandlerResolved(handler.name)

        val schemaVersion = envelope?.schemaVersion ?: PgmqEnvelope.DEFAULT_SCHEMA_VERSION
        if (!handler.supports(schemaVersion)) {
            val reason = "schemaVersion $schemaVersion is not supported by handler " +
                "'${handler.name}' (expected ${handler.schemaVersions})"
            log.warn("msg_id={} in {}: {}", raw.msgId, spec.queue, reason)
            deadLetter(raw, reason, DeadLetterReason.SCHEMA_VERSION)
            return Outcome.DEAD_LETTERED
        }

        return try {
            invoke(handler, raw, envelope)
            processed.incrementAndGet()
            Outcome.OK
        } catch (e: Throwable) {
            onHandlerFailure(raw, e)
        }
    }

    private fun resolveEnvelope(raw: PgmqMessage<String>): PgmqEnvelope? = when (spec.envelopeValidation) {
        EnvelopeValidation.OFF -> raw.envelope
        EnvelopeValidation.LENIENT -> raw.envelope.also {
            if (it == null) {
                log.debug(
                    "msg_id={} in {} has no complete envelope; LENIENT lets it through.",
                    raw.msgId, spec.queue,
                )
            }
        }
        EnvelopeValidation.STRICT -> raw.envelope
            ?: throw PgmqEnvelopeException(
                "Message has no complete envelope. Queue '${spec.queue}' runs with STRICT — set " +
                    "envelopeValidation to LENIENT or OFF for messages from foreign clients.",
            )
    }

    private fun resolveHandler(label: String?): RegisteredHandler<*>? =
        (label?.let { labelled[it] }) ?: catchAll

    @Suppress("UNCHECKED_CAST")
    private fun invoke(handler: RegisteredHandler<*>, raw: PgmqMessage<String>, envelope: PgmqEnvelope?) {
        val typed = raw.withPayload(convert(handler, raw.payload))
        val context = ContainerContext(raw)

        // The envelope stays in the context for the duration of the handler, so a `send` from
        // inside it inherits correlationId and sets causationId by itself.
        PgmqExchangeContext.with(envelope) {
            (handler as RegisteredHandler<Any?>).handle(listOf(typed) as List<PgmqMessage<Any?>>, context)
        }

        if (context.acknowledged) return
        when (spec.ackMode) {
            AckMode.DELETE -> template.delete(spec.queue, raw.msgId)
            AckMode.ARCHIVE -> template.archive(spec.queue, raw.msgId)
            AckMode.MANUAL -> log.debug(
                "msg_id={} in {}: MANUAL without acknowledgement — the message will be " +
                    "redelivered once the visibility timeout expires.",
                raw.msgId, spec.queue,
            )
        }
    }

    /** Raw handlers bypass the serializer entirely and see the stored JSON text. */
    private fun convert(handler: RegisteredHandler<*>, rawJson: String): Any? =
        if (handler.raw) rawJson else template.convertPayload(rawJson, handler.payloadType)

    private fun onHandlerFailure(raw: PgmqMessage<String>, error: Throwable): Outcome {
        failed.incrementAndGet()

        if (isPermanent(error)) {
            log.warn(
                "msg_id={} in {} failed permanently ({}) — dead lettered straight away, no retry.",
                raw.msgId, spec.queue, error.javaClass.simpleName, error,
            )
            deadLetter(
                raw,
                "${error.javaClass.name}: ${error.message}",
                DeadLetterReason.PERMANENT_FAILURE,
            )
            return Outcome.DEAD_LETTERED
        }

        if (raw.readCount > spec.maxRetries) {
            log.warn(
                "msg_id={} in {} given up after {} deliveries — dead lettered.",
                raw.msgId, spec.queue, raw.readCount, error,
            )
            deadLetter(
                raw,
                "maxRetries=${spec.maxRetries} exhausted: ${error.message}",
                DeadLetterReason.RETRIES_EXHAUSTED,
            )
            return Outcome.DEAD_LETTERED
        }

        val backoff = retryBackoff(raw.readCount)
        log.warn(
            "msg_id={} in {} failed (attempt {}/{}), retrying in {}.",
            raw.msgId, spec.queue, raw.readCount, spec.maxRetries + 1, backoff, error,
        )
        runCatching { template.setVisibilityTimeout(spec.queue, raw.msgId, backoff) }
            .onFailure { log.error("Could not set the retry backoff for msg_id={}.", raw.msgId, it) }

        return Outcome.FAILED_RETRYABLE
    }

    private fun isPermanent(error: Throwable): Boolean {
        var cause: Throwable? = error
        while (cause != null) {
            if (cause is PgmqPermanentException ||
                cause is PgmqEnvelopeException ||
                cause is PgmqDeserializationException ||
                cause.javaClass.isAnnotationPresent(PgmqNonRetryable::class.java) ||
                cause.javaClass.name in spec.nonRetryableExceptions
            ) {
                return true
            }
            cause = cause.cause?.takeIf { it !== cause }
        }
        return false
    }

    private fun retryBackoff(attempt: Int): Duration {
        val exponent = (attempt - 1).coerceIn(0, 20)
        val candidate = BASE_RETRY_BACKOFF * (1L shl exponent).toDouble()
        return minOf(candidate, spec.maxRetryBackoff)
    }

    private fun handleUnroutable(raw: PgmqMessage<String>, label: String?) {
        val reason = "No handler for label '${label ?: "<none>"}' and no catch-all in queue " +
            "'${spec.queue}'"
        when (spec.unroutable) {
            UnroutableMessagePolicy.DLQ -> {
                log.warn("msg_id={}: {} — dead lettering it.", raw.msgId, reason)
                deadLetter(raw, reason, DeadLetterReason.UNROUTABLE)
            }
            UnroutableMessagePolicy.ARCHIVE -> {
                log.warn("msg_id={}: {} — archiving it.", raw.msgId, reason)
                template.archive(spec.queue, raw.msgId)
            }
            UnroutableMessagePolicy.IGNORE -> {
                log.debug("msg_id={}: {} — discarding it.", raw.msgId, reason)
                template.delete(spec.queue, raw.msgId)
            }
        }
    }

    /**
     * Moves the message to the dead letter queue and archives the original.
     *
     * The envelope survives and gains diagnostic headers, so a replay stays possible and the reason
     * for sorting the message out remains visible. [reason] is free text for a human, [category] the
     * coarse classification that is safe to use as a metric tag.
     */
    private fun deadLetter(raw: PgmqMessage<String>, reason: String, category: DeadLetterReason) {
        val dlq = spec.effectiveDeadLetterQueue
        try {
            template.createQueueIfMissing(dlq)
            template.sendToDeadLetter(
                deadLetterQueue = dlq,
                source = raw,
                diagnostics = mapOf(
                    PgmqHeaderNames.DLQ_REASON to reason.take(MAX_REASON_LENGTH),
                    PgmqHeaderNames.DLQ_ORIGIN_QUEUE to spec.queue,
                    PgmqHeaderNames.DLQ_ORIGIN_MSG_ID to raw.msgId.toString(),
                    PgmqHeaderNames.DLQ_READ_CT to raw.readCount.toString(),
                ),
            )
            template.archive(spec.queue, raw.msgId)
            deadLettered.incrementAndGet()
            metrics.messageDeadLettered(spec.queue, category)
        } catch (e: Throwable) {
            log.error(
                "msg_id={} could not be moved to the dead letter queue '{}'; it stays in '{}' " +
                    "and will be redelivered.",
                raw.msgId, dlq, spec.queue, e,
            )
        }
    }

    private fun releaseImmediately(messages: List<PgmqMessage<String>>) {
        if (messages.isEmpty()) return
        runCatching {
            template.setVisibilityTimeout(spec.queue, messages.map { it.msgId }, Duration.ZERO)
        }.onFailure {
            log.warn(
                "{} messages of {} could not be released immediately; they will only become " +
                    "visible again once the visibility timeout expires.",
                messages.size, spec.queue, it,
            )
        }
    }

    /** Interruptible wait. Returns `false` when the thread should stop. */
    private fun sleepInterruptible(duration: Duration): Boolean = try {
        if (duration > Duration.ZERO) Thread.sleep(duration.inWholeMilliseconds)
        true
    } catch (_: InterruptedException) {
        Thread.currentThread().interrupt()
        false
    }

    private inner class ContainerContext(private val raw: PgmqMessage<String>) : PgmqContext {
        var acknowledged: Boolean = false
            private set

        override val queue: String get() = spec.queue
        override val message: PgmqMessage<*> get() = raw

        override fun ack() {
            template.delete(spec.queue, raw.msgId)
            acknowledged = true
        }

        override fun archive() {
            template.archive(spec.queue, raw.msgId)
            acknowledged = true
        }

        override fun retryAfter(delay: Duration) {
            template.setVisibilityTimeout(spec.queue, raw.msgId, delay)
            acknowledged = true
        }
    }

    private companion object {
        val PAUSE_CHECK_INTERVAL = 200.milliseconds
        val BASE_RETRY_BACKOFF = 1.seconds
        const val MAX_REASON_LENGTH = 500

        /** Metric label for a message that never reached a handler. */
        const val UNROUTED_LABEL = "unrouted"
    }
}
