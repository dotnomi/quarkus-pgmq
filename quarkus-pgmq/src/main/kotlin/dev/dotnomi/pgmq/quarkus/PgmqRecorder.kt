package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.PgmqMessage
import dev.dotnomi.pgmq.envelope.EnvelopeValidation
import dev.dotnomi.pgmq.listener.AckMode
import dev.dotnomi.pgmq.listener.ListenerSpec
import dev.dotnomi.pgmq.listener.PgmqContext
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar
import dev.dotnomi.pgmq.listener.ReadStrategy
import dev.dotnomi.pgmq.listener.RegisteredHandler
import dev.dotnomi.pgmq.listener.UnroutableMessagePolicy
import dev.dotnomi.pgmq.serializer.PgmqType
import dev.dotnomi.pgmq.topics.SubscriptionMode
import io.quarkus.arc.Arc
import io.quarkus.runtime.annotations.RecordableConstructor
import io.quarkus.runtime.annotations.Recorder
import org.eclipse.microprofile.config.ConfigProvider
import java.lang.reflect.Method
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

/**
 * Listener metadata collected at build time and replayed at startup.
 *
 * Values are carried as strings because the annotation attributes are strings — that is what makes
 * `${config.key}` expressions possible. They are resolved in [PgmqRecorder], not here.
 */
class RecordedListener @RecordableConstructor constructor(
    val beanClassName: String,
    val methodName: String,
    val payloadClassName: String,
    val signature: String,
    val queue: String,
    val label: String,
    val client: String,
    val concurrency: String,
    val batchSize: String,
    val pollInterval: String,
    val messageInterval: String,
    val visibilityTimeout: String,
    val vtRefresh: String,
    val ackMode: String,
    val maxRetries: String,
    val deadLetterQueue: String,
    val autoCreate: String,
    val autoStart: String,
    val fifo: String,
    val schemaVersions: String,
    val envelopeValidation: String,
    val unroutable: String,
    val batch: String,
    /** Non-blank turns this into a topic listener; the queue is then resolved at startup. */
    val topic: String = "",
    val topicGroup: String = "",
    val topicMode: String = "SHARED",
    /** Skips deserialization and hands the payload over as raw JSON text. */
    val raw: String = "false",
) {
    companion object {
        /** How the handler method wants its arguments. */
        const val SIGNATURE_PAYLOAD: String = "PAYLOAD"
        const val SIGNATURE_MESSAGE: String = "MESSAGE"
        const val SIGNATURE_PAYLOAD_CONTEXT: String = "PAYLOAD_CONTEXT"
        const val SIGNATURE_MESSAGE_CONTEXT: String = "MESSAGE_CONTEXT"
        const val SIGNATURE_BATCH: String = "BATCH"
    }
}

/**
 * Turns build-time metadata into live listener containers.
 *
 * Registration happens here rather than in a `StartupEvent` observer so that the containers exist
 * before anything can reference them; **starting** them is deliberately deferred to [PgmqLifecycle],
 * which runs last among all startup observers.
 */
// `open` because Quarkus subclasses recorders during augmentation; Kotlin classes are final by
// default and the extension processor rejects that.
@Recorder
open class PgmqRecorder {

    // `open` for the same reason as the class: Quarkus intercepts recorder calls via a generated
    // subclass, which a final method would prevent.
    open fun registerListeners(listeners: List<RecordedListener>) {
        // Only stored here. Creating queues and topic subscriptions needs a live connection, and the
        // datasource bean is not initialised at RUNTIME_INIT — PgmqLifecycle does that work later.
        Arc.container().instance(RecordedListeners::class.java).get().addAll(listeners)
    }

    /** Supplies the implementation of a [PgmqPublisher] interface as a synthetic bean. */
    open fun publisherProxy(
        publisherInterface: String,
    ): java.util.function.Function<io.quarkus.arc.SyntheticCreationalContext<Any>, Any> =
        java.util.function.Function {
            PgmqPublisherProxy.create(Class.forName(publisherInterface, true, classLoader()))
        }

    /**
     * Builds the containers. Called from the startup observer, where the datasource is usable.
     */
    open fun materialise(
        registrar: PgmqListenerRegistrar,
        templates: PgmqTemplateRegistry,
        recorded: List<RecordedListener>,
    ) {
        if (recorded.any { it.topic.isNotBlank() }) {
            templates.defaultTemplate().topics().initialiseSchema()
        }

        val config = Arc.container().instance(PgmqRuntimeConfig::class.java).get()

        recorded.forEach { listener ->
            val queue = listener.resolveQueue(templates)
            registrar.register(listener.toSpec(queue, config), listener.toHandler())
        }
    }

    /**
     * For a topic listener the physical queue only exists once the subscription does, so the
     * subscribe happens here rather than at build time.
     */
    private fun RecordedListener.resolveQueue(templates: PgmqTemplateRegistry): String {
        if (topic.isBlank()) return resolve(queue)

        val resolvedClient = client.takeIf { it.isNotBlank() }?.let { resolve(it) }
            ?: ListenerSpec.DEFAULT_CLIENT
        val template = templates.template(resolvedClient)
        val group = topicGroup.takeIf { it.isNotBlank() }?.let { resolve(it) }
            ?: ConfigProvider.getConfig()
                .getOptionalValue("quarkus.application.name", String::class.java)
                .orElse("application")

        val subscription = template.topics().subscribe(
            topic = resolve(topic),
            group = group,
            mode = SubscriptionMode.valueOf(topicMode),
        )

        // Recorded so the heartbeat can name this exact subscriber row later: for a BROADCAST
        // subscription the name contains the instance id, which only exists once we have subscribed.
        Arc.container().instance(PgmqOwnedSubscriptions::class.java).get()
            .record(resolvedClient, subscription)

        return subscription.queue
    }

    private fun RecordedListener.toSpec(resolvedQueue: String, config: PgmqRuntimeConfig): ListenerSpec {
        val id = if (client.isBlank()) resolvedQueue else "${resolve(client)}/$resolvedQueue"
        val fifoEnabled = resolveBoolean(fifo, false)

        return ListenerSpec(
            queue = resolvedQueue,
            client = client.takeIf { it.isNotBlank() }?.let { resolve(it) } ?: ListenerSpec.DEFAULT_CLIENT,
            // Configuration wins over the annotation here, unlike for publishers: a listener's
            // throughput settings are exactly what an operator needs to tune per environment without
            // a rebuild, which is what `pgmq.listener.<id>.*` is documented to do.
            concurrency = override(id, "concurrency", Int::class.javaObjectType)
                ?: resolveInt(concurrency, 1),
            batchSize = override(id, "batch-size", Int::class.javaObjectType)
                ?: resolveInt(batchSize, 1),
            pollInterval = overrideDuration(id, "poll-interval")
                ?: resolveDuration(pollInterval, ListenerSpec.DEFAULT_VISIBILITY_TIMEOUT),
            messageInterval = overrideDuration(id, "message-interval")
                ?: resolveDuration(messageInterval, Duration.ZERO),
            visibilityTimeout = overrideDuration(id, "visibility-timeout")
                ?: visibilityTimeout.takeIf { it.isNotBlank() }
                    ?.let { resolveDuration(it, ListenerSpec.DEFAULT_VISIBILITY_TIMEOUT) },
            vtRefresh = override(id, "vt-refresh", Boolean::class.javaObjectType)
                ?: resolveBoolean(vtRefresh, false),
            ackMode = AckMode.valueOf(ackMode),
            maxRetries = override(id, "max-retries", Int::class.javaObjectType)
                ?: resolveInt(maxRetries, 5),
            deadLetterQueue = override(id, "dead-letter-queue", String::class.java)
                ?: deadLetterQueue.takeIf { it.isNotBlank() }?.let { resolve(it) },
            autoCreateQueue = resolveBoolean(autoCreate, true),
            autoStart = override(id, "auto-start", Boolean::class.javaObjectType)
                ?: resolveBoolean(autoStart, true),
            readStrategy = if (fifoEnabled) ReadStrategy.GROUPED else ReadStrategy.PLAIN,
            envelopeValidation = EnvelopeValidation.valueOf(envelopeValidation),
            unroutable = UnroutableMessagePolicy.valueOf(unroutable),
            // Application-wide settings. Without passing them here they would be inert config keys:
            // the container reads both, but nothing ever populated them.
            shutdownTimeout = config.shutdownTimeout,
            nonRetryableExceptions = config.nonRetryableExceptions,
        )
    }

    private fun RecordedListener.toHandler(): RegisteredHandler<Any?> {
        val beanClass = Class.forName(beanClassName, true, classLoader())
        val payloadClass = loadPayloadClass(payloadClassName)
        val method = findMethod(beanClass)

        return RegisteredHandler(
            label = label.takeIf { it.isNotBlank() }?.let { resolve(it) },
            payloadType = PgmqType.of(payloadClass) as PgmqType<Any?>,
            batch = resolveBoolean(batch, false),
            raw = resolveBoolean(raw, false),
            schemaVersions = parseSchemaVersions(schemaVersions),
            name = "${beanClass.simpleName}#$methodName",
            invoke = { messages, context -> invoke(beanClass, method, messages, context) },
        )
    }

    private fun RecordedListener.invoke(
        beanClass: Class<*>,
        method: Method,
        messages: List<PgmqMessage<Any?>>,
        context: PgmqContext,
    ) {
        val bean = Arc.container().instance(beanClass).get()
            ?: error(
                "No CDI bean available for ${beanClass.name}. A @PgmqListener method must live in a " +
                    "bean — add @ApplicationScoped or an equivalent scope to the class.",
            )

        when (signature) {
            RecordedListener.SIGNATURE_BATCH -> method.invoke(bean, messages)
            RecordedListener.SIGNATURE_MESSAGE -> messages.forEach { method.invoke(bean, it) }
            RecordedListener.SIGNATURE_MESSAGE_CONTEXT -> messages.forEach { method.invoke(bean, it, context) }
            RecordedListener.SIGNATURE_PAYLOAD_CONTEXT -> messages.forEach { method.invoke(bean, it.payload, context) }
            else -> messages.forEach { method.invoke(bean, it.payload) }
        }
    }

    private fun RecordedListener.findMethod(beanClass: Class<*>): Method =
        beanClass.declaredMethods.firstOrNull { it.name == methodName }
            ?.also { it.isAccessible = true }
            ?: error("Method '$methodName' not found on ${beanClass.name}.")

    // --- Value resolution ---

    /**
     * Resolves a `${config.key}` expression against the runtime configuration; any other value is
     * taken literally. This is what lets an annotation attribute be overridden per environment
     * without recompiling.
     */
    private fun resolve(value: String): String {
        if (!value.startsWith("\${") || !value.endsWith("}")) return value
        val expression = value.substring(2, value.length - 1)
        val (key, fallback) = expression.split(":", limit = 2).let {
            it[0] to it.getOrNull(1)
        }
        return ConfigProvider.getConfig().getOptionalValue(key, String::class.java)
            .orElseGet {
                fallback ?: error(
                    "Configuration key '$key' is referenced by a pgmq annotation but not set, and " +
                        "no default was given. Use \${$key:someDefault} to provide one.",
                )
            }
    }

    /** Reads `pgmq.listener.<id>.<property>`, or `null` when it is not configured. */
    private fun <T> override(id: String, property: String, type: Class<T>): T? =
        ConfigProvider.getConfig()
            .getOptionalValue("pgmq.listener.$id.$property", type)
            .orElse(null)

    private fun overrideDuration(id: String, property: String): Duration? =
        override(id, property, java.time.Duration::class.java)?.toKotlinDuration()

    private fun resolveInt(value: String, default: Int): Int =
        resolve(value).trim().takeIf { it.isNotEmpty() }?.toIntOrNull() ?: default

    private fun resolveBoolean(value: String, default: Boolean): Boolean =
        resolve(value).trim().takeIf { it.isNotEmpty() }?.toBooleanStrictOrNull() ?: default

    private fun resolveDuration(value: String, default: Duration): Duration {
        val resolved = resolve(value).trim()
        if (resolved.isEmpty()) return default
        // Accepts both the compact form ("5s", "250ms") and ISO-8601 ("PT5S"), matching what Quarkus
        // configuration properties allow elsewhere.
        return runCatching { io.quarkus.runtime.configuration.DurationConverter().convert(resolved).toKotlinDuration() }
            .getOrElse {
                error("'$resolved' is not a valid duration. Use forms like '5s', '250ms' or 'PT1M'.")
            }
    }

    private fun parseSchemaVersions(value: String): IntRange? {
        val trimmed = value.trim()
        if (trimmed.isEmpty()) return null
        return if (trimmed.contains("..")) {
            val (from, to) = trimmed.split("..", limit = 2)
            IntRange(from.trim().toInt(), to.trim().toInt())
        } else {
            val single = trimmed.toInt()
            IntRange(single, single)
        }
    }

    private fun loadPayloadClass(name: String): Class<*> = when (name) {
        "int" -> Int::class.javaObjectType
        "long" -> Long::class.javaObjectType
        "boolean" -> Boolean::class.javaObjectType
        "double" -> Double::class.javaObjectType
        else -> Class.forName(name, true, classLoader())
    }

    private fun classLoader(): ClassLoader =
        Thread.currentThread().contextClassLoader ?: javaClass.classLoader
}
