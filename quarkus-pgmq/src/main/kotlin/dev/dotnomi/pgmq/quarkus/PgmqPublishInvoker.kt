package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.CorrelationPolicy
import dev.dotnomi.pgmq.PgmqExchangeContext
import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.envelope.PgmqEnvelope
import dev.dotnomi.pgmq.listener.ListenerSpec
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.ConfigProvider
import java.lang.reflect.Method
import java.lang.reflect.Parameter
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinDuration

/**
 * Turns a [PgmqPublisher] method call into a publish.
 *
 * Shared by the interceptor, which handles publishers declared as concrete classes, and by the
 * generated proxy for publishers declared as interfaces.
 *
 * Precedence, strongest to weakest:
 * ```
 * PgmqMessageCustomizer   (runs last, always wins)
 *   > annotated parameter (@PgmqLabel, @PgmqTarget, @PgmqDelay, @PgmqGroup, @PgmqHeader)
 *   > method annotation
 *   > type annotation
 *   > configuration (pgmq.publisher.<name>.*)
 *   > library default
 * ```
 */
@Singleton
class PgmqPublishInvoker(
    private val templates: PgmqTemplateRegistry,
    private val config: PgmqRuntimeConfig,
) {

    fun invoke(method: Method, arguments: Array<Any?>): Any? {
        val settings = resolveSettings(method)

        val builder = PgmqMessageBuilder(
            label = settings.label,
            targetId = settings.targetId,
            sourceId = settings.sourceId,
            schemaVersion = settings.schemaVersion,
            correlationId = correlationFor(settings.correlation),
            causationId = causationFor(settings.correlation),
        )
        builder.delay(settings.delay)
        settings.headers.forEach { (name, value) -> builder.header(name, value) }

        val payload = mapParameters(method, arguments, builder)

        // The customizer runs last, so it always wins over annotations and configuration.
        arguments.filterIsInstance<PgmqMessageCustomizer>().forEach { it.customize(builder) }

        val envelope = builder.buildEnvelope()
        // Resolved per call so a publisher annotated with client = "analytics" really writes to
        // that datasource rather than the default one.
        val msgId = templates.template(settings.client).sendEnvelope(
            queue = settings.queue,
            payload = payload,
            envelope = envelope,
            headers = builder.userHeaders(),
            group = builder.group,
            delay = builder.delay,
        )

        return when (method.returnType) {
            Long::class.javaPrimitiveType, java.lang.Long::class.java -> msgId
            List::class.java -> listOf(msgId)
            else -> null
        }
    }

    /**
     * Splits the argument list into the payload and everything that belongs on the envelope.
     *
     * - a single unannotated parameter *is* the payload
     * - several unannotated parameters become a JSON object keyed by parameter name
     * - annotated parameters and the customizer never reach the payload
     */
    private fun mapParameters(
        method: Method,
        arguments: Array<Any?>,
        builder: PgmqMessageBuilder,
    ): Any? {
        val payloadParts = LinkedHashMap<String, Any?>()
        var singlePayload: Any? = null
        var singlePayloadSeen = false
        var payloadCount = 0

        method.parameters.forEachIndexed { index, parameter ->
            val value = arguments.getOrNull(index)

            when {
                parameter.type == PgmqMessageCustomizer::class.java -> Unit

                parameter.isAnnotationPresent(PgmqLabel::class.java) ->
                    builder.label(value?.toString())

                parameter.isAnnotationPresent(PgmqTarget::class.java) ->
                    value?.toString()?.let { builder.targetId(it) }

                parameter.isAnnotationPresent(PgmqGroup::class.java) ->
                    builder.group(value?.toString())

                parameter.isAnnotationPresent(PgmqDelay::class.java) ->
                    toDuration(value)?.let { builder.delay(it) }

                parameter.isAnnotationPresent(PgmqHeader::class.java) -> {
                    val name = parameter.getAnnotation(PgmqHeader::class.java).value
                    value?.let { builder.header(name, it.toString()) }
                }

                else -> {
                    payloadCount++
                    singlePayload = value
                    singlePayloadSeen = true
                    payloadParts[parameterName(parameter, index)] = value
                }
            }
        }

        return when {
            payloadCount == 1 && singlePayloadSeen -> singlePayload
            payloadCount == 0 -> null
            else -> payloadParts
        }
    }

    /**
     * Fails rather than falling back to `arg0`, `arg1`, ….
     *
     * A positional fallback would silently produce a payload with the wrong field names — valid JSON
     * that no consumer can read, and nothing anywhere reporting a problem. That is exactly the kind
     * of defect a native image could introduce (parameter metadata stripped from the binary) and it
     * must not pass unnoticed.
     */
    private fun parameterName(parameter: Parameter, index: Int): String {
        check(parameter.isNamePresent) {
            "Parameter #$index of a @PgmqPublisher method has no name at runtime, so the payload " +
                "field names cannot be derived. Compile the declaring class with '-parameters' " +
                "(Java) or '-java-parameters' (Kotlin), or take a single payload object instead of " +
                "several parameters."
        }
        return parameter.name
    }

    private fun toDuration(value: Any?): Duration? = when (value) {
        null -> null
        is Duration -> value
        is java.time.Duration -> value.toKotlinDuration()
        is Number -> value.toLong().seconds
        is String -> runCatching {
            io.quarkus.runtime.configuration.DurationConverter().convert(value).toKotlinDuration()
        }.getOrNull()
        else -> null
    }

    private fun correlationFor(policy: CorrelationPolicy): String? = when (policy) {
        CorrelationPolicy.NEW -> null
        CorrelationPolicy.INHERIT -> PgmqExchangeContext.inherited().correlationId
    }

    private fun causationFor(policy: CorrelationPolicy): String? = when (policy) {
        CorrelationPolicy.NEW -> null
        CorrelationPolicy.INHERIT -> PgmqExchangeContext.inherited().causationId
    }

    // --- Settings resolution ---

    private class Settings(
        val queue: String,
        val client: String,
        val label: String?,
        val targetId: String,
        val sourceId: String,
        val schemaVersion: Int,
        val delay: Duration,
        val headers: Map<String, String>,
        val correlation: CorrelationPolicy,
    )

    private fun resolveSettings(method: Method): Settings {
        val onMethod: PgmqPublisher? = method.getAnnotation(PgmqPublisher::class.java)
        val onType: PgmqPublisher? = method.declaringClass.getAnnotation(PgmqPublisher::class.java)
        val name = publisherName(method)

        fun pick(select: (PgmqPublisher) -> String): String? =
            onMethod?.let(select)?.takeIf { it.isNotBlank() }
                ?: onType?.let(select)?.takeIf { it.isNotBlank() }

        val queue = pick { it.queue }?.let(::expand)
            ?: configString(name, "queue")
            ?: throw IllegalStateException(
                "@PgmqPublisher on ${method.declaringClass.simpleName}#${method.name} has no queue, " +
                    "and neither the enclosing type nor pgmq.publisher.$name.queue provides one.",
            )

        val headers = buildMap {
            onType?.headers?.forEach { put(it.substringBefore('='), it.substringAfter('=', "")) }
            onMethod?.headers?.forEach { put(it.substringBefore('='), it.substringAfter('=', "")) }
        }

        return Settings(
            queue = queue,
            client = pick { it.client }?.let(::expand)
                ?: configString(name, "client")
                ?: ListenerSpec.DEFAULT_CLIENT,
            label = pick { it.label }?.let(::expand) ?: configString(name, "label"),
            targetId = pick { it.targetId }?.let(::expand)
                ?: configString(name, "target-id")
                ?: PgmqTemplate.DEFAULT_TARGET_ID,
            sourceId = pick { it.sourceId }?.let(::expand)
                ?: configString(name, "source-id")
                ?: config.sourceId,
            schemaVersion = pick { it.schemaVersion }?.toIntOrNull()
                ?: configString(name, "schema-version")?.toIntOrNull()
                ?: PgmqEnvelope.DEFAULT_SCHEMA_VERSION,
            delay = pick { it.delay }?.let { toDuration(it) }
                ?: configString(name, "delay")?.let { toDuration(it) }
                ?: Duration.ZERO,
            headers = headers,
            correlation = onMethod?.correlation ?: onType?.correlation ?: CorrelationPolicy.INHERIT,
        )
    }

    /** Method name in kebab-case, the key under `pgmq.publisher.<name>.*`. */
    private fun publisherName(method: Method): String = method.name
        .replace(Regex("([a-z0-9])([A-Z])"), "$1-$2")
        .lowercase()

    private fun configString(publisher: String, property: String): String? =
        ConfigProvider.getConfig()
            .getOptionalValue("pgmq.publisher.$publisher.$property", String::class.java)
            .orElse(null)

    /** Resolves a `${config.key}` expression; anything else is taken literally. */
    private fun expand(value: String): String {
        if (!value.startsWith("\${") || !value.endsWith("}")) return value
        val expression = value.substring(2, value.length - 1)
        val key = expression.substringBefore(':')
        val fallback = expression.substringAfter(':', "").takeIf { expression.contains(':') }
        return ConfigProvider.getConfig().getOptionalValue(key, String::class.java)
            .orElseGet {
                fallback ?: throw IllegalStateException(
                    "Configuration key '$key' is referenced by a @PgmqPublisher attribute but not " +
                        "set, and no default was given. Use \${$key:someDefault} to provide one.",
                )
            }
    }
}
