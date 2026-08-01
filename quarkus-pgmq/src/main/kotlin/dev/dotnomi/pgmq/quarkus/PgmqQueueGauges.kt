package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Backlog gauges, fed by a background poller rather than by the scrape.
 *
 * These are the only pgmq metrics that cost a query: `pgmq.metrics()` does a `count(*)` per queue,
 * which grows expensive on exactly the backlogged queue whose depth you most want to know. Polling
 * on a fixed interval and letting the gauges read the cached value decouples that cost from the
 * scrape frequency, so a second Prometheus server does not double the database load.
 *
 * | Gauge | Tags | Meaning |
 * |---|---|---|
 * | `pgmq.queue.length` | `queue`, `role` | messages waiting, invisible ones included |
 * | `pgmq.queue.oldest.message.age.seconds` | `queue`, `role` | age of the oldest message |
 * | `pgmq.listener.inflight` | `queue` | messages currently inside a handler |
 *
 * `role` is `main` or `dlq`, so one query separates the two without knowing the naming convention.
 *
 * The age is what to alert on. Length says little by itself — a thousand messages being drained at
 * speed are fine, three stuck for ten minutes are not.
 */
@Singleton
class PgmqQueueGauges(
    private val registrar: PgmqListenerRegistrar,
    private val templates: PgmqTemplateRegistry,
    private val registry: MeterRegistry,
    private val config: PgmqRuntimeConfig,
) : PgmqQueueGaugeSupport {
    private val log = LoggerFactory.getLogger(PgmqQueueGauges::class.java)

    private val gauges = ConcurrentHashMap<String, AtomicLong>()
    private var executor: ScheduledExecutorService? = null

    override fun start() {
        if (executor != null) return

        // Daemon thread: a hanging poll must never keep the JVM alive at shutdown.
        val service = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "pgmq-queue-gauges").apply { isDaemon = true }
        }
        executor = service
        service.scheduleWithFixedDelay(
            ::refresh,
            0,
            config.metricsQueuePollInterval.inWholeMilliseconds,
            TimeUnit.MILLISECONDS,
        )
        log.debug("Queue gauges polling every {}.", config.metricsQueuePollInterval)
    }

    override fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    /**
     * One pass over every registered listener.
     *
     * Every failure is swallowed after logging: a metrics poller must not be able to disturb message
     * processing, and a dead letter queue that does not exist yet is the normal case, not an error.
     */
    private fun refresh() {
        registrar.listeners().forEach { listener ->
            val template = runCatching { templates.template(listener.client) }.getOrElse { return@forEach }

            set("pgmq.listener.inflight", listener.queue, null, listener.stats.inFlight.toLong())
            publishQueue(template, listener.queue, role = "main", quiet = false)
            publishQueue(template, listener.deadLetterQueue, role = "dlq", quiet = true)
        }
    }

    private fun publishQueue(template: PgmqTemplate, queue: String, role: String, quiet: Boolean) {
        val metrics = runCatching { template.metrics(queue) }.getOrElse { error ->
            if (!quiet) log.debug("Could not read metrics for queue '{}'.", queue, error)
            return
        }
        set("pgmq.queue.length", queue, role, metrics.length)
        set(
            "pgmq.queue.oldest.message.age.seconds",
            queue,
            role,
            metrics.oldestMessageAgeSeconds?.toLong() ?: 0L,
        )
    }

    /** Registers the gauge on first sight and updates the value it reads from thereafter. */
    private fun set(name: String, queue: String, role: String?, value: Long) {
        gauges.computeIfAbsent("$name/$queue") {
            AtomicLong().also { holder ->
                Gauge.builder(name, holder) { it.get().toDouble() }
                    .tag("queue", queue)
                    .apply { role?.let { tag("role", it) } }
                    .register(registry)
            }
        }.set(value)
    }
}
