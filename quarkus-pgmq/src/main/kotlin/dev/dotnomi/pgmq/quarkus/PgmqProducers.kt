package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.listener.ListenerSpec
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar
import io.quarkus.arc.DefaultBean
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.context.Dependent
import jakarta.enterprise.inject.spi.InjectionPoint
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.util.Optional
import javax.sql.DataSource
import kotlin.time.toKotlinDuration

/** Supplies the beans an application injects. */
@ApplicationScoped
class PgmqProducers {

    /** The default template. [DefaultBean], so an application can replace it. */
    @Produces
    @Singleton
    @DefaultBean
    fun pgmqTemplate(registry: PgmqTemplateRegistry): PgmqTemplate = registry.defaultTemplate()

    /** Template for a named client, e.g. `@Inject @PgmqClient("analytics")`. */
    @Produces
    @Dependent
    @PgmqClient("")
    fun namedTemplate(
        injectionPoint: InjectionPoint,
        registry: PgmqTemplateRegistry,
    ): PgmqTemplate {
        val client = injectionPoint.qualifiers
            .filterIsInstance<PgmqClient>()
            .firstOrNull()
            ?.value
            ?: ListenerSpec.DEFAULT_CLIENT
        return registry.template(client)
    }

    /** Resolves each listener's client through the registry. */
    @Produces
    @Singleton
    @DefaultBean
    fun listenerRegistrar(
        registry: PgmqTemplateRegistry,
        metrics: PgmqMetricsHolder,
    ): PgmqListenerRegistrar =
        PgmqListenerRegistrar({ client -> registry.template(client) }, metrics.resolved)
}

/**
 * Starts and stops the listener containers with the application.
 *
 * The startup observer has the highest priority, so it is notified last — after migrations, cache
 * warm-up and the application's own initialisation.
 */
@ApplicationScoped
class PgmqLifecycle(
    private val registrar: PgmqListenerRegistrar,
    private val templates: PgmqTemplateRegistry,
    private val template: PgmqTemplate,
    private val config: PgmqRuntimeConfig,
    private val recorded: RecordedListeners,
    private val topicMaintenance: PgmqTopicMaintenance,
    private val metrics: PgmqMetricsHolder,
) {
    private val log = LoggerFactory.getLogger(PgmqLifecycle::class.java)

    fun onStart(@Observes @Priority(Int.MAX_VALUE) event: StartupEvent) {
        if (config.verifyExtension) verifyExtension()
        metrics.verifyAvailable()

        if (!config.listenersEnabled) {
            log.info("pgmq.listeners-enabled=false — no listener will consume. Publishing still works.")
            return
        }

        // Built here, not in the recorder: this needs a live datasource, which RUNTIME_INIT lacks.
        PgmqRecorder().materialise(registrar, templates, recorded.all())

        registrar.startAll()

        // After the subscriptions exist, which the heartbeat needs to know about.
        topicMaintenance.start()

        // After the containers exist, since the gauges iterate over registered listeners.
        if (config.metricsEnabled && config.metricsQueueGauges) metrics.startQueueGauges()

        val listeners = registrar.listeners()
        if (listeners.isEmpty()) {
            log.debug("No pgmq listeners registered.")
        } else {
            log.info(
                "pgmq listeners ready: {}",
                listeners.joinToString { "${it.id} (${it.state})" },
            )
        }
    }

    fun onStop(@Observes event: ShutdownEvent) {
        metrics.stopQueueGauges()
        topicMaintenance.stop()
        registrar.stopAll()

        // A per-instance queue is useless once this instance is gone; the janitor is the fallback.
        recorded.topicListeners().forEach { listener ->
            runCatching {
                val group = listener.topicGroup.takeIf { it.isNotBlank() }
                    ?: config.applicationNameOrDefault
                templates.template(listener.client.ifBlank { ListenerSpec.DEFAULT_CLIENT })
                    .topics()
                    .unsubscribe(listener.topic, group)
            }.onFailure { log.warn("Could not unsubscribe from topic '{}'.", listener.topic, it) }
        }
    }

    /** Fails fast, so an incompatible installation does not surface as a puzzling SQL error. */
    private fun verifyExtension() {
        val version = template.extensionVersion()
            ?: error(
                "The pgmq extension is not installed in this database. Run " +
                    "'CREATE EXTENSION pgmq;' or set pgmq.verify-extension=false if the check is " +
                    "running against the wrong datasource.",
            )

        val parts = version.split(".").mapNotNull { it.toIntOrNull() }
        val major = parts.getOrElse(0) { 0 }
        val minor = parts.getOrElse(1) { 0 }

        check(major > 1 || (major == 1 && minor >= MINIMUM_MINOR)) {
            "pgmq $version is too old. The message envelope lives in the 'headers' column, which " +
                "was introduced in pgmq 1.$MINIMUM_MINOR.0. Upgrade the extension, or set " +
                "pgmq.verify-extension=false and envelopeValidation=OFF on every listener to run " +
                "without envelopes."
        }

        log.debug("pgmq extension version {} verified.", version)
    }

    private companion object {
        /** `headers`, and therefore the envelope, exists from pgmq 1.5.0 onwards. */
        const val MINIMUM_MINOR = 5
    }
}

