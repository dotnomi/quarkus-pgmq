package dev.dotnomi.pgmq.listener

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.metrics.PgmqMetrics
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Registers listeners and controls them by id.
 *
 * The controllable unit is the queue, not the handler method: stopping one of several handlers would
 * leave the consumer reading messages it can no longer route. [stop] stops reading entirely and the
 * messages stay in the queue.
 */
class PgmqListenerRegistrar(
    /** Resolves a client name to its template. */
    private val templateResolver: (client: String) -> PgmqTemplate,
    /** Handed to every container this registrar builds. */
    private val metrics: PgmqMetrics = PgmqMetrics.NOOP,
) {
    private val log = LoggerFactory.getLogger(PgmqListenerRegistrar::class.java)

    private val containers = ConcurrentHashMap<String, PgmqListenerContainer>()
    private val pending = ConcurrentHashMap<String, MutableList<RegisteredHandler<*>>>()
    private val specs = ConcurrentHashMap<String, ListenerSpec>()

    @Volatile
    private var started: Boolean = false

    /** Single-template setup. Rejects a named client instead of silently using the default. */
    constructor(template: PgmqTemplate) : this({ client ->
        require(client == ListenerSpec.DEFAULT_CLIENT) {
            "Listener requests client '$client', but this registrar was built with a single " +
                "template. Named clients (several datasources) are not wired up yet — either drop " +
                "the client attribute or construct PgmqListenerRegistrar with a resolver that maps " +
                "client names to templates."
        }
        template
    })

    // --- Registration ---

    /**
     * Registers a handler. Repeated calls for one queue share a container.
     *
     * @return the container id: `<queue>`, or `<client>/<queue>` for a named client
     */
    fun <T> register(spec: ListenerSpec, handler: RegisteredHandler<T>): String {
        val id = spec.id

        specs.compute(id) { _, existing ->
            if (existing == null) {
                spec
            } else {
                requireConsistent(existing, spec)
                existing
            }
        }

        pending.computeIfAbsent(id) { mutableListOf() }.let { handlers ->
            synchronized(handlers) { handlers += handler }
        }

        if (started) rebuild(id)
        return id
    }

    /** These settings apply per queue, so all handlers of a queue must agree on them. */
    private fun requireConsistent(existing: ListenerSpec, incoming: ListenerSpec) {
        val conflicts = buildList {
            fun <V> check(name: String, a: V, b: V) { if (a != b) add("$name: $a vs. $b") }
            check("concurrency", existing.concurrency, incoming.concurrency)
            check("batchSize", existing.batchSize, incoming.batchSize)
            check("pollInterval", existing.pollInterval, incoming.pollInterval)
            check("messageInterval", existing.messageInterval, incoming.messageInterval)
            check("visibilityTimeout", existing.visibilityTimeout, incoming.visibilityTimeout)
            check("ackMode", existing.ackMode, incoming.ackMode)
            check("autoStart", existing.autoStart, incoming.autoStart)
            check("readStrategy", existing.readStrategy, incoming.readStrategy)
            check("maxRetries", existing.maxRetries, incoming.maxRetries)
            check("deadLetterQueue", existing.effectiveDeadLetterQueue, incoming.effectiveDeadLetterQueue)
            check("envelopeValidation", existing.envelopeValidation, incoming.envelopeValidation)
        }

        require(conflicts.isEmpty()) {
            "Conflicting settings for queue '${existing.queue}': $conflicts. These values apply " +
                "per queue, because every handler on a queue shares one container — they must match " +
                "across all of them."
        }
    }

    /** Stops the container and removes it with all its handlers. */
    fun unregister(id: String) {
        containers.remove(id)?.stop()
        pending.remove(id)
        specs.remove(id)
        log.info("Listener {} unregistered.", id)
    }

    private fun rebuild(id: String) {
        val spec = specs[id] ?: return
        val handlers = pending[id]?.let { synchronized(it) { it.toList() } } ?: return
        if (handlers.isEmpty()) return

        val wasRunning = containers[id]?.state() == ListenerState.RUNNING
        containers.remove(id)?.stop()

        val container = PgmqListenerContainer(spec, templateResolver(spec.client), handlers, metrics)
        containers[id] = container
        if (wasRunning || (started && spec.autoStart)) container.start()
    }

    // --- Control ---

    /**
     * Builds all containers and starts those with `autoStart = true`.
     *
     * Call once the application is fully initialised, so no listener processes a message while
     * migrations are still running.
     */
    fun startAll() {
        started = true
        specs.keys.forEach { id ->
            if (containers[id] == null) {
                val spec = specs[id]!!
                val handlers = pending[id]?.let { synchronized(it) { it.toList() } } ?: emptyList()
                if (handlers.isEmpty()) return@forEach
                containers[id] =
                    PgmqListenerContainer(spec, templateResolver(spec.client), handlers, metrics)
            }
        }

        containers.values.forEach { container ->
            if (container.spec.autoStart) {
                container.start()
            } else {
                log.info(
                    "Listener {} has autoStart = false and reads nothing until start(\"{}\") is called.",
                    container.id, container.id,
                )
            }
        }
    }

    fun stopAll() {
        started = false
        containers.values.forEach { runCatching { it.stop() }.onFailure { e -> log.warn("Stopping a listener failed.", e) } }
    }

    fun listeners(): List<PgmqListenerInfo> = containers.values
        .map { it.info() }
        .sortedBy { it.id }

    fun start(id: String): Unit = container(id).start()

    fun stop(id: String): Unit = container(id).stop()

    fun pause(id: String): Unit = container(id).pause()

    fun resume(id: String): Unit = container(id).resume()

    fun state(id: String): ListenerState = container(id).state()

    fun isRunning(id: String): Boolean = container(id).state() == ListenerState.RUNNING

    private fun container(id: String): PgmqListenerContainer = containers[id]
        ?: throw IllegalArgumentException(
            "No listener with id '$id'. Known ids are ${containers.keys.sorted()}.",
        )
}
