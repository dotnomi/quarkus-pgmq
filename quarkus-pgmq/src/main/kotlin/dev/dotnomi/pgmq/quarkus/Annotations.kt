package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.CorrelationPolicy
import dev.dotnomi.pgmq.envelope.EnvelopeValidation
import dev.dotnomi.pgmq.listener.AckMode
import dev.dotnomi.pgmq.listener.ReadStrategy
import dev.dotnomi.pgmq.listener.UnroutableMessagePolicy
import dev.dotnomi.pgmq.topics.SubscriptionMode

/**
 * Marks a method as a consumer of a PGMQ queue.
 *
 * All handlers of a queue share one consumer, so [concurrency], [batchSize], [pollInterval],
 * [messageInterval], [visibilityTimeout], [ackMode], [autoStart] and [fifo] apply **per queue**;
 * conflicting values across handlers fail the build.
 *
 * Attributes are `String` so they accept a `${config.key}` expression.
 *
 * Supported signatures:
 * ```
 * fun handle(payload: OrderDto)
 * fun handle(message: PgmqMessage<OrderDto>)
 * fun handle(payload: OrderDto, context: PgmqContext)
 * fun handle(batch: List<PgmqMessage<OrderDto>>)   // batch = "true"
 * ```
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqListener(
    /** Queue to consume. Supports `${config.key}` expressions. */
    val queue: String,

    /**
     * Message type this handler claims. Empty makes it the catch-all of its queue, which picks up
     * everything no labelled handler claims. At most one catch-all per queue.
     */
    val label: String = "",

    /**
     * Named Quarkus datasource to consume from (`quarkus.datasource.<client>.jdbc.url`). Empty uses
     * the default. Also becomes part of the listener id.
     */
    val client: String = "",

    /** Parallel worker threads. */
    val concurrency: String = "1",

    /**
     * Messages fetched per read. A larger batch trades redelivery latency for fewer round-trips: on
     * an ungraceful kill up to `batchSize - 1` messages wait for the visibility timeout.
     */
    val batchSize: String = "1",

    /** Wait time only when the queue was empty. */
    val pollInterval: String = "5s",

    /** Minimum spacing between processing starts, shared across the queue's workers. */
    val messageInterval: String = "0s",

    /**
     * Empty derives it from [batchSize] and [messageInterval]. An explicit value that cannot cover
     * the batch duration fails at startup.
     */
    val visibilityTimeout: String = "",

    /** Extends the visibility timeout during processing. */
    val vtRefresh: String = "false",

    val ackMode: AckMode = AckMode.DELETE,

    /** Number of failed deliveries before the message is dead lettered. */
    val maxRetries: String = "5",

    /** Empty derives the name as `<queue>_dlq`. */
    val deadLetterQueue: String = "",

    val autoCreate: String = "true",

    /** With `"false"` the container exists as `NOT_STARTED` and reads nothing until `start(id)`. */
    val autoStart: String = "true",

    /**
     * Ordering within an `x-pgmq-group`. Safe with `concurrency > 1`; a failure aborts the rest of
     * the group instead of running ahead of it.
     */
    val fifo: String = "false",

    /**
     * Versions this handler understands, as `"1"` or `"1..3"`. Empty accepts any; anything else is
     * dead lettered without retry.
     */
    val schemaVersions: String = "",

    val envelopeValidation: EnvelopeValidation = EnvelopeValidation.STRICT,

    /** What happens to a message no handler claims. */
    val unroutable: UnroutableMessagePolicy = UnroutableMessagePolicy.DLQ,

    /** Hands the whole batch to the method at once. Requires a `List<PgmqMessage<T>>` parameter. */
    val batch: String = "false",

    /**
     * Hands the payload over as the raw JSON text instead of deserializing it.
     *
     * The parameter must then be `String`. Useful when the message is only routed or forwarded, where
     * parsing it would be wasted work, and when the shape is unknown or varies.
     */
    val raw: String = "false",
)

/**
 * Turns a method into a publish call. **The method body is not executed.**
 *
 * Can be placed on the type to set defaults for its methods; a method annotation overrides them.
 * `messageId`, `sendingTime` and `causationId` have no attribute because they are library-managed.
 *
 * Parameter mapping:
 * - one parameter → it *is* the payload
 * - several parameters → a JSON object built from the Kotlin parameter names
 * - [PgmqLabel], [PgmqTarget], [PgmqDelay], [PgmqGroup] set the corresponding envelope field;
 *   [PgmqHeader] sets a free user header. None of them end up in the payload.
 * - a parameter of type [PgmqMessageCustomizer] is recognised by **type**, not position
 * - return type decides: `Unit` → fire and forget, `Long` → the `msg_id`, `List<Long>` → batch
 */
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@jakarta.interceptor.InterceptorBinding
annotation class PgmqPublisher(
    /** Target queue. Required either here or on the enclosing type. */
    @get:jakarta.enterprise.util.Nonbinding val queue: String = "",

    /** Named datasource to publish to. See [PgmqListener.client]. */
    @get:jakarta.enterprise.util.Nonbinding val client: String = "",

    /** Fixed message type for this publisher. */
    @get:jakarta.enterprise.util.Nonbinding val label: String = "",

    /** Target system. Typically set once at type level. */
    @get:jakarta.enterprise.util.Nonbinding val targetId: String = "",

    /** Overrides `pgmq.source-id`. */
    @get:jakarta.enterprise.util.Nonbinding val sourceId: String = "",

    /** Payload schema version this publisher produces. */
    @get:jakarta.enterprise.util.Nonbinding val schemaVersion: String = "",

    /** Default delay before the message becomes visible, e.g. `"30s"`. */
    @get:jakarta.enterprise.util.Nonbinding val delay: String = "",

    /** Fixed user headers as `["priority=high"]`. */
    @get:jakarta.enterprise.util.Nonbinding val headers: Array<String> = [],

    /** Whether this publisher continues the current flow or starts a new one. */
    @get:jakarta.enterprise.util.Nonbinding val correlation: CorrelationPolicy = CorrelationPolicy.INHERIT,
)

/**
 * Consumes a topic: subscribes on startup, unsubscribes on shutdown.
 *
 * A topic fans out onto one real queue per subscriber, so the queue is resolved at startup.
 *
 * ```
 * // every replica receives every message
 * @PgmqTopicListener(topic = "orders", mode = SubscriptionMode.BROADCAST)
 * fun notify(order: OrderDto) { … }
 *
 * // the replicas share the work, each message handled once
 * @PgmqTopicListener(topic = "orders", group = "billing")
 * fun bill(order: OrderDto) { … }
 * ```
 *
 * Handler signatures and per-queue settings match [PgmqListener].
 *
 * A subscriber that is not registered yet misses messages, as with SNS or Redis pub/sub. For
 * [SubscriptionMode.BROADCAST], whose queue does not outlive its instance, anything published while
 * the instance was down is gone.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqTopicListener(
    /** Topic to subscribe to. Supports `${config.key}` expressions. */
    val topic: String,

    /** Subscriber group. Empty uses `quarkus.application.name`. */
    val group: String = "",

    val mode: SubscriptionMode = SubscriptionMode.SHARED,

    /** See [PgmqListener.label]. */
    val label: String = "",

    val client: String = "",
    val concurrency: String = "1",
    val batchSize: String = "1",
    val pollInterval: String = "5s",
    val messageInterval: String = "0s",
    val visibilityTimeout: String = "",
    val vtRefresh: String = "false",
    val ackMode: AckMode = AckMode.DELETE,
    val maxRetries: String = "5",
    val deadLetterQueue: String = "",
    val autoStart: String = "true",
    val fifo: String = "false",
    val schemaVersions: String = "",
    val envelopeValidation: EnvelopeValidation = EnvelopeValidation.STRICT,
    val unroutable: UnroutableMessagePolicy = UnroutableMessagePolicy.DLQ,
    val batch: String = "false",

    /** See [PgmqListener.raw]. */
    val raw: String = "false",
)

/** Sets a free user header from a parameter. Reserved names are rejected at build time. */
@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqHeader(val value: String)

/** Sets the envelope's `label` from a parameter. */
@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqLabel

/** Sets the envelope's `targetId` from a parameter. */
@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqTarget

/** Sets the publish delay from a parameter of type `Duration`, `java.time.Duration` or `Long` (seconds). */
@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqDelay

/** Sets the FIFO group (`x-pgmq-group`) from a parameter. */
@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class PgmqGroup

/** Injects the [PgmqTemplate] of a named datasource. */
@Target(AnnotationTarget.FIELD, AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@jakarta.inject.Qualifier
annotation class PgmqClient(
    // Nonbinding so one producer serves every client name; the value is read from the injection point.
    @get:jakarta.enterprise.util.Nonbinding val value: String,
)

