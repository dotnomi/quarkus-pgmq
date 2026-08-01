package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.metrics.PgmqMetrics
import jakarta.enterprise.inject.Instance
import jakarta.inject.Singleton

/**
 * Start/stop contract for the backlog gauges, free of any Micrometer type.
 *
 * [PgmqMetricsHolder] must be loadable when Micrometer is absent, so it may not mention
 * `PgmqQueueGauges` — whose constructor signature references `MeterRegistry` — directly.
 */
interface PgmqQueueGaugeSupport {
    fun start()
    fun stop()
}

/**
 * Single point that decides whether metrics are recorded at all.
 *
 * Micrometer is an optional dependency: the beans behind these injection points exist only when the
 * application brought a registry along and `pgmq.metrics.enabled=true`. Everything is resolved
 * through [Instance] so that their absence is an ordinary state rather than a startup failure —
 * except when metrics were explicitly asked for, which is checked in [verifyAvailable].
 */
@Singleton
class PgmqMetricsHolder(
    private val config: PgmqRuntimeConfig,
    private val metrics: Instance<PgmqMetrics>,
    private val gauges: Instance<PgmqQueueGaugeSupport>,
) {
    /** The recorder to hand to templates and containers; a no-op unless metrics are on. */
    val resolved: PgmqMetrics
        get() = if (metrics.isResolvable) metrics.get() else PgmqMetrics.NOOP

    /**
     * Turning metrics on without a registry present would silently record nothing, which is worse
     * than not starting: the dashboards stay empty and nobody knows why.
     */
    fun verifyAvailable() {
        check(!config.metricsEnabled || metrics.isResolvable) {
            "pgmq.metrics.enabled=true, but no Micrometer registry is present. Add a registry " +
                "extension — for Prometheus that is quarkus-micrometer-registry-prometheus — or " +
                "set pgmq.metrics.enabled=false."
        }
    }

    fun startQueueGauges() {
        if (gauges.isResolvable) gauges.get().start()
    }

    fun stopQueueGauges() {
        if (gauges.isResolvable) gauges.get().stop()
    }
}
