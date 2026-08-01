package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.listener.ListenerSpec
import io.quarkus.arc.Arc
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

/**
 * Resolves a client name to the [PgmqTemplate] of the matching datasource.
 *
 * A client is a named Quarkus datasource:
 * ```properties
 * quarkus.datasource.jdbc.url=jdbc:postgresql://.../main          # default client
 * quarkus.datasource.analytics.jdbc.url=jdbc:postgresql://.../analytics
 * ```
 * ```kotlin
 * @PgmqListener(queue = "events", client = "analytics")
 * fun onEvent(event: EventDto) { … }
 * ```
 *
 * Templates are cached per client.
 */
@ApplicationScoped
class PgmqTemplateRegistry(
    private val config: PgmqRuntimeConfig,
    private val metrics: PgmqMetricsHolder,
) {

    private val log = LoggerFactory.getLogger(PgmqTemplateRegistry::class.java)
    private val templates = ConcurrentHashMap<String, PgmqTemplate>()

    /** The template for the default datasource. */
    fun defaultTemplate(): PgmqTemplate = template(ListenerSpec.DEFAULT_CLIENT)

    /**
     * The template for [client], created on first use.
     *
     * @throws IllegalStateException if no datasource is configured under that name
     */
    fun template(client: String): PgmqTemplate {
        val key = client.ifBlank { ListenerSpec.DEFAULT_CLIENT }
        return templates.computeIfAbsent(key) { name ->
            PgmqTemplate(
                dataSource = resolveDataSource(name),
                sourceId = config.sourceId,
                metrics = metrics.resolved,
            )
                .also { log.debug("Created PgmqTemplate for client '{}'.", name) }
        }
    }

    /** Client names with a template created so far. */
    fun knownClients(): Set<String> = templates.keys.toSet()

    private fun resolveDataSource(client: String): DataSource {
        val container = Arc.container()

        if (client == ListenerSpec.DEFAULT_CLIENT) {
            val handle = container.instance(DataSource::class.java)
            check(handle.isAvailable) {
                "No default datasource is configured. Set quarkus.datasource.jdbc.url, or point " +
                    "every listener and publisher at a named client."
            }
            return handle.get()
        }

        val handle = container.instance(
            DataSource::class.java,
            io.quarkus.agroal.DataSource.DataSourceLiteral(client),
        )
        check(handle.isAvailable) {
            "No datasource named '$client' is configured. A pgmq client maps one-to-one onto a " +
                "Quarkus datasource, so add quarkus.datasource.$client.jdbc.url (plus username and " +
                "password) — or remove client = \"$client\" to use the default datasource."
        }
        return handle.get()
    }

}
