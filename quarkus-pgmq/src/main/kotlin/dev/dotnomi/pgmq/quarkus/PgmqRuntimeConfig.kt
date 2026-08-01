package dev.dotnomi.pgmq.quarkus

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.ConfigProvider
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.util.Optional
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinDuration

/**
 * Application-wide runtime configuration.
 *
 * Per-listener and per-publisher settings are looked up programmatically instead, since their keys
 * contain ids only known after annotation scanning.
 */
@ApplicationScoped
class PgmqRuntimeConfig(
    /** Envelope `sourceId` when a publisher does not set one. */
    @param:ConfigProperty(name = "pgmq.source-id")
    private val configuredSourceId: Optional<String>,

    @param:ConfigProperty(name = "quarkus.application.name")
    private val applicationName: Optional<String>,

    /** Identity used to name per-instance broadcast queues. Falls back to `HOSTNAME`. */
    @param:ConfigProperty(name = "pgmq.instance-id")
    private val configuredInstanceId: Optional<String>,

    /** Whether listeners consume at all. Publishing is unaffected. */
    @param:ConfigProperty(name = "pgmq.listeners-enabled", defaultValue = "true")
    val listenersEnabled: Boolean,

    /** Checks the installed pgmq version at startup; the envelope needs 1.5.0 or newer. */
    @param:ConfigProperty(name = "pgmq.verify-extension", defaultValue = "true")
    val verifyExtension: Boolean,

    /** How long shutdown waits for an in-flight message. */
    @param:ConfigProperty(name = "pgmq.shutdown-timeout", defaultValue = "PT30S")
    private val configuredShutdownTimeout: java.time.Duration,

    /** Exception class names treated as permanent, alongside `@PgmqNonRetryable`. */
    @param:ConfigProperty(name = "pgmq.non-retryable")
    private val configuredNonRetryable: Optional<List<String>>,

    /** Publishes Micrometer metrics. Requires a Micrometer registry on the classpath. */
    @param:ConfigProperty(name = "pgmq.metrics.enabled", defaultValue = "false")
    val metricsEnabled: Boolean,

    /** The backlog gauges, the only metrics that cost a database query. */
    @param:ConfigProperty(name = "pgmq.metrics.queue-gauges", defaultValue = "true")
    val metricsQueueGauges: Boolean,

    /** How often the backlog gauges are refreshed. */
    @param:ConfigProperty(name = "pgmq.metrics.queue-poll-interval", defaultValue = "PT30S")
    private val configuredMetricsQueuePollInterval: java.time.Duration,
) {
    val metricsQueuePollInterval: Duration
        get() = configuredMetricsQueuePollInterval.toKotlinDuration()

    /** Falls back to a literal so topic groups stay stable when the name is unset. */
    val applicationNameOrDefault: String
        get() = applicationName.orElseGet { "application" }

    val sourceId: String
        get() = configuredSourceId.orElseGet { applicationName.orElse(DEFAULT_SOURCE_ID) }

    val instanceId: String
        get() = configuredInstanceId
            .orElseGet { System.getenv("HOSTNAME") ?: dev.dotnomi.pgmq.UuidV7.generateString() }

    val shutdownTimeout: Duration
        get() = configuredShutdownTimeout.toKotlinDuration()

    val nonRetryableExceptions: Set<String>
        get() = configuredNonRetryable.orElseGet { emptyList() }.toSet()

    companion object {
        const val DEFAULT_SOURCE_ID: String = "unknown"

        /** Reads a per-listener override such as `pgmq.listener.mails.message-interval`. */
        fun <T> listenerOverride(id: String, property: String, type: Class<T>): Optional<T> =
            ConfigProvider.getConfig().getOptionalValue("pgmq.listener.$id.$property", type)

        /** Same for publishers, keyed by the method name in kebab-case. */
        fun <T> publisherOverride(name: String, property: String, type: Class<T>): Optional<T> =
            ConfigProvider.getConfig().getOptionalValue("pgmq.publisher.$name.$property", type)

        val DEFAULT_POLL_INTERVAL: Duration = 5.seconds
    }
}
