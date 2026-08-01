# quarkus-pgmq

[![Build](https://github.com/dotnomi/quarkus-pgmq/actions/workflows/build.yml/badge.svg)](https://github.com/dotnomi/quarkus-pgmq/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.dotnomi/quarkus-pgmq)](https://central.sonatype.com/artifact/io.github.dotnomi/quarkus-pgmq)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

A Kotlin library and Quarkus extension for [PGMQ](https://github.com/pgmq/pgmq), the Postgres message
queue extension. Publishing, consuming, queue administration, topics, and per-queue permissions.

Two artifacts to choose from:

| Module | Use it when |
|---|---|
| `pgmq-core` | Any JVM application. Needs only a `javax.sql.DataSource`. |
| `quarkus-pgmq` | Quarkus applications. Adds `@PgmqListener`, `@PgmqPublisher`, `@PgmqTopicListener`. |

A third module, `quarkus-pgmq-deployment`, is published as well. Never depend on it directly — Quarkus
resolves it during your build, and the extension does not work without it being available.

Requires **pgmq 1.5.0 or newer** — the message envelope lives in the `headers` column, which older
versions do not have. The extension checks this at startup and refuses to run otherwise.

---

## Contents

- [Setup](#setup)
- [Publishing](#publishing)
  - [Declaratively](#declaratively) · [As an interface](#as-an-interface)
- [Consuming](#consuming)
- [The message envelope](#the-message-envelope)
- [How consuming actually works](#how-consuming-actually-works)
- [Visibility timeout: the duplicate-processing trap](#visibility-timeout-the-duplicate-processing-trap)
- [Failures, retries and the dead letter queue](#failures-retries-and-the-dead-letter-queue)
- [Ordering (FIFO)](#ordering-fifo)
- [Topics](#topics)
- [Several datasources](#several-datasources)
- [Permissions](#permissions)
- [Controlling listeners at runtime](#controlling-listeners-at-runtime)
- [Metrics](#metrics)
- [Configuration reference](#configuration-reference)
- [Common mistakes](#common-mistakes)
- [What this library does not do](#what-this-library-does-not-do)
- [Native image](#native-image)

---

## Setup

```kotlin
dependencies {
    implementation("io.github.dotnomi:quarkus-pgmq:0.1.0")
}
```

Without Quarkus, the core library stands on its own:

```kotlin
dependencies {
    implementation("io.github.dotnomi:pgmq-core:0.1.0")
}
```

```properties
quarkus.datasource.db-kind=postgresql
quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/app
quarkus.datasource.username=app
quarkus.datasource.password=secret
```

The extension uses your existing Agroal datasource. Nothing else to configure — queues are created on
demand.

Outside Quarkus:

```kotlin
val template = PgmqTemplate(dataSource, sourceId = "my-service")
```

---

## Publishing

### Directly

```kotlin
@Inject lateinit var pgmq: PgmqTemplate

pgmq.send("orders", Order("A-1", 4999), label = "OrderCreated", targetId = "fulfilment")
pgmq.sendBatch("orders", orders)
pgmq.send("mails", mail, delay = 30.seconds)
pgmq.sendAt("reminders", reminder, visibleAt = Instant.parse("2026-01-01T09:00:00Z"))
```

### Declaratively

```kotlin
@ApplicationScoped
@PgmqPublisher(queue = "mail", targetId = "mailer")
class MailPublisher {

    fun sendMail(recipient: String, text: String) { /* never executed */ }

    @PgmqPublisher(label = "Urgent", schemaVersion = "2")
    fun sendUrgent(mail: MailDto, @PgmqHeader("priority") priority: Int): Long { /* never executed */ }
}
```

**The method body is not executed.** The extension intercepts the call and publishes instead. Do not
put logic in the body — it will never run.

### As an interface

Since the body is dead anyway, declare the publisher as an interface and there is no body to begin
with:

```kotlin
@PgmqPublisher(queue = "mail", targetId = "mailer")
interface MailPublisher {

    fun sendMail(mail: MailDto)

    fun sendUrgent(mail: MailDto, @PgmqLabel label: String, @PgmqHeader("priority") priority: Int): Long

    /** A default method keeps its own body and really runs. */
    fun sendGreeting(name: String) = sendMail(MailDto("greetings@example.com", "Hello $name"))
}
```

```kotlin
@Inject lateinit var mails: MailPublisher
```

`default` methods are the useful half of this: put preparation in a `default` method and let it
delegate to an abstract one, and you get real code and publishing in the same type without any dead
bodies.

**An abstract class does not work** — only an interface or a concrete class. An abstract class can
neither be instantiated by CDI nor proxied, and saying so at build time is better than the unsatisfied
injection point you would otherwise get.

Which form to pick: the interface makes the "no body" rule impossible to get wrong and is the better
default. Take a concrete class when the publisher also needs injected collaborators or state of its
own.

Under the hood the two differ — a class is intercepted, an interface gets a generated implementation,
because CDI cannot instantiate an interface. From the outside that is invisible.

Parameter mapping:

| Parameters | Payload |
|---|---|
| one unannotated | that object *is* the payload |
| several unannotated | a JSON object keyed by parameter name |
| `@PgmqLabel`, `@PgmqTarget`, `@PgmqDelay`, `@PgmqGroup` | envelope field, never the payload |
| `@PgmqHeader("x")` | free user header, never the payload |
| `PgmqMessageCustomizer` | recognised by type, never the payload |

Return type decides the result: `Unit` for fire-and-forget, `Long` for the `msg_id`, `List<Long>` for
a batch.

### Per-call adjustments

Prefer parameter annotations — they are type-safe and checked at build time. For the rest:

```kotlin
@PgmqPublisher(queue = "test")
fun sendTest(text: String, customize: PgmqMessageCustomizer = PgmqMessageCustomizer.NONE) {}

sendTest("Hello") { it.header("priority", "high").label("Urgent").delay(30.seconds) }
```

The builder deliberately cannot set `messageId`, `sendingTime` or `causationId` — see
[the envelope](#the-message-envelope).

### Precedence

```
PgmqMessageCustomizer  >  annotated parameter  >  method annotation
                       >  type annotation      >  configuration  >  default
```

Configuration only *fills in* what the annotations leave blank; it does not override an explicit
annotation value. To make an annotation attribute environment-specific, use an expression:

```kotlin
@PgmqPublisher(queue = "\${orders.queue:orders}")
```

### Transactions

`send` joins an active JTA transaction. A rolled-back transaction publishes nothing, which is all you
need for the outbox pattern:

```kotlin
@Transactional
fun placeOrder(order: Order) {
    repository.persist(order)
    pgmq.send("orders", order)   // committed together with the insert, or not at all
}
```

Outside Quarkus, use `inTransaction`:

```kotlin
pgmq.inTransaction { tx ->
    tx.send("orders", first)
    tx.send("orders", second)
}
```

---

## Consuming

```kotlin
@ApplicationScoped
class OrderConsumer {

    @PgmqListener(queue = "orders")
    fun onOrder(order: Order) { ... }
}
```

Supported signatures:

```kotlin
fun handle(payload: T)
fun handle(message: PgmqMessage<T>)
fun handle(payload: T, context: PgmqContext)
fun handle(message: PgmqMessage<T>, context: PgmqContext)
fun handle(batch: List<PgmqMessage<T>>)     // requires batch = "true"
```

Anything else fails the build with a message listing the supported shapes.

### Payload types

The payload is stored as `jsonb`, so what you may declare depends on the message shape:

| Declared type | JSON string body | JSON object body |
|---|---|---|
| a DTO | — | deserialized by Jackson |
| `String` | the unquoted string | **fails**, dead lettered |
| `JsonNode` | the node | the node |
| `String` with `raw = "true"` | the stored text, quoted | the stored text |

`String` is fine when the body really is a JSON string — a fixed marker, an id, a line of text. It
cannot take an object, because Jackson has nowhere to put it.

#### Taking the JSON text verbatim

`raw = "true"` hands the payload over exactly as stored and skips Jackson altogether. Use it when the
message is only routed, forwarded or logged, where parsing it is work you never needed, and when the
shape is unknown or varies:

```kotlin
@PgmqListener(queue = "inbound", raw = "true", envelopeValidation = EnvelopeValidation.OFF)
fun route(json: String) {
    forwardTo(pickTarget(json), json)
}
```

The parameter must be `String`; anything else fails the build. `raw` works on
`@PgmqTopicListener` too, and combines with every other attribute — the envelope, labels, retries and
dead lettering behave exactly as they do for a deserialized payload.

The same thing at runtime:

```kotlin
listeners.register(
    ListenerSpec(queue = "adhoc", pollInterval = 1.seconds),
    pgmqRawHandler { json -> handleMyself(json) },
)

// with the metadata as well
pgmqRawMessageHandler { message ->
    println("${message.envelope?.label}: ${message.payload}")
}
```

`JsonNode` is the middle ground: it also takes any shape, but parses it. Reach for it when you need
to look *into* the message; reach for `raw` when you do not.

### Several handlers on one queue

Use `label` to route:

```kotlin
@PgmqListener(queue = "events", label = "OrderCreated")
fun onCreated(event: OrderCreated) { ... }

@PgmqListener(queue = "events", label = "OrderCancelled")
fun onCancelled(event: OrderCancelled) { ... }

@PgmqListener(queue = "events")        // no label: catch-all for this queue
fun onAnythingElse(event: JsonNode) { ... }
```

A handler without a label is the **catch-all**: it receives everything no labelled handler claims,
including messages with no label at all. At most one catch-all per queue.

> **All handlers of a queue share one consumer.** That is a necessity, not an implementation detail:
> `label` lives in the message headers, and pgmq's server-side `conditional` filter only matches the
> message body. Separate consumers would read each other's messages, hide them behind a visibility
> timeout, and discard them.
>
> Consequence: `concurrency`, `batchSize`, `pollInterval`, `messageInterval`, `visibilityTimeout`,
> `ackMode`, `autoStart` and `fifo` apply **per queue**. If two handlers on the same queue disagree
> on any of them, the build fails.

---

## The message envelope

Every message published through this library carries eight header fields:

| Field | Set by | Purpose |
|---|---|---|
| `messageId` | library | UUIDv7, stable across retries, replays and topic fan-out. The idempotency key. |
| `sourceId` | config / publisher | Originating system or tenant. |
| `targetId` | publisher | Intended recipient. An **assertion**, not a router. |
| `label` | publisher | Optional message type. Drives handler dispatch. |
| `correlationId` | inherited or generated | Ties one logical flow together. |
| `causationId` | library | `messageId` of the message that caused this one. |
| `schemaVersion` | publisher | Payload format version. |
| `sendingTime` | library | ISO-8601 UTC with milliseconds. |

They sit flat in pgmq's `headers` jsonb, so `headers->>'label'` works in psql. Your own headers live
alongside them; the eight names plus anything under `x-pgmq-` and `x-dlq-` are reserved and rejected.

### Correlation is automatic

Publishing from inside a handler inherits the flow and records causality without any code:

```kotlin
@PgmqListener(queue = "points", label = "PointsAwarded")
fun onPointsAwarded(event: PointsAwarded) {
    mails.sendMail(event.userId, "You earned ${event.points} points")
}
```

| Message | `messageId` | `correlationId` | `causationId` |
|---|---|---|---|
| `PointsAwarded` | `m-1` | `c-1` (new) | `null` |
| `SendMail` | `m-2` | `c-1` (inherited) | `m-1` |

`correlationId` answers *what belongs together*, `causationId` answers *what caused what*. When one
event turns into 400 messages, the first tells you they are related; the second tells you 397 came
from one handler, which is where the bug is.

To start a fresh flow deliberately — a scheduler entry point, for instance:

```kotlin
@PgmqPublisher(queue = "digests", correlation = CorrelationPolicy.NEW)
fun sendDigest(digest: DigestDto) {}
```

### Why three fields are not settable

`messageId` must be unique or consumer deduplication breaks. `sendingTime` is always now.
`causationId` comes from the processing context — overwriting it severs the causal chain. None of
them have an annotation attribute or a builder method, because an option that must be rejected is
worse than no option.

### Foreign messages

A message published by another client, or by hand in psql, has no envelope. Choose per listener:

```kotlin
@PgmqListener(queue = "external", envelopeValidation = EnvelopeValidation.LENIENT)
```

`STRICT` (default) dead-letters it, `LENIENT` processes it anyway, `OFF` skips the check entirely.

---

## How consuming actually works

Polling, not push. You control the rate:

```
read → empty?  → wait pollInterval → read again
     → messages? → process → read again immediately
```

```kotlin
@PgmqListener(
    queue = "mails",
    pollInterval = "5s",        // only when the queue was empty
    messageInterval = "1s",     // minimum spacing between messages
    batchSize = "5",
)
fun sendMail(mail: MailDto) { ... }
```

`pollInterval` applies **only to an empty queue**. With messages present, reading continues without
pause, so a backlog of 100 000 drains at full speed rather than in `pollInterval` steps.

`messageInterval` throttles: at `1s`, messages start one second apart. It is a **pacer**, not a sleep
— with 300 ms of processing it waits 700 ms, so the effective rate really is 1/s and does not drift.

> **The pacer is shared across a queue's workers.** `messageInterval = 1s` with `concurrency = 4`
> still yields one message per second in total, with up to four in flight. A per-worker pacer would
> have quadrupled the rate.

`concurrency` is the number of worker threads for the queue. Worth raising when handlers wait on
something external (SMTP, HTTP); pointless beyond core count for pure computation. With
`concurrency > 1` ordering is no longer guaranteed — see [FIFO](#ordering-fifo).

Each queue gets its own named thread pool (`pgmq-<queue>-N`), so a slow listener cannot starve others.

---

## Visibility timeout: the duplicate-processing trap

`pgmq.read` does not delete a message; it hides it for `visibilityTimeout`. If it is not acknowledged
in time, it becomes visible again and **another consumer processes it**.

This is the single most common source of duplicate processing, and it needs no crash at all:

```
batchSize = 10, messageInterval = 1s   → the last message starts ~10s after the read
visibilityTimeout = 5s                 → messages 6-10 became visible again long ago
```

The library protects you three ways:

1. **Derived by default.** Leave `visibilityTimeout` unset and it is computed from
   `batchSize × messageInterval` with headroom.
2. **Validated when explicit.** A value that cannot cover the batch duration fails at startup with a
   message naming the numbers.
3. **`vtRefresh = "true"`** extends the timeout of still-pending messages during processing.

```kotlin
@PgmqListener(queue = "slow", messageInterval = "2s", batchSize = "10", vtRefresh = "true")
```

### Batch size

Defaults to **1**. A crash does not lose fetched messages — they return once the timeout expires —
but a larger batch delays up to `batchSize - 1` messages on an ungraceful kill. Since acknowledgement
is per message, raising it only saves `read` queries, so the trade is worse than it looks. Raise it
when throughput matters more than redelivery latency.

### Shutdown

On `SIGTERM` the listener stops reading, finishes the message in flight, and releases the rest of the
batch immediately with `setVt(0)` so the next pod picks them up at once. Without that, every deploy
would leave a gap the length of the visibility timeout.

---

## Failures, retries and the dead letter queue

Failures are split into two kinds, because some cannot be fixed by trying again:

**Permanent — dead lettered on the first attempt.** Payload does not deserialize, envelope invalid,
unsupported `schemaVersion`, unknown label with no catch-all. Plus anything you mark:

```kotlin
@PgmqNonRetryable
class InvalidRecipientException(message: String) : RuntimeException(message)
```

```properties
pgmq.non-retryable=com.example.InvalidRecipientException
```

**Transient — retried with exponential backoff** until `maxRetries`, then dead lettered.

Without the distinction, a message with broken JSON would burn `maxRetries × backoff` — minutes —
before anyone learns about it.

The dead letter queue is `<queue>_dlq` unless you name one. The message keeps its envelope and gains
diagnostics, so a replay is possible and the reason is visible:

```kotlin
val dead = pgmq.readRaw("orders_dlq").single()
dead.diagnostics["x-dlq-reason"]         // why it was rejected
dead.diagnostics["x-dlq-origin-queue"]
dead.diagnostics["x-dlq-origin-msg-id"]
```

Note that these are *not* in `dead.headers` — they are reserved names, and `headers` holds only your
own.

### Acknowledgement

`ackMode = AckMode.DELETE` (default) removes the message, `ARCHIVE` moves it to pgmq's archive table,
`MANUAL` leaves it to you:

```kotlin
@PgmqListener(queue = "audit", ackMode = AckMode.MANUAL)
fun onEvent(event: Event, context: PgmqContext) {
    if (shouldDefer(event)) {
        context.retryAfter(5.minutes)
        return
    }
    process(event)
    context.archive()
}
```

---

## Ordering (FIFO)

pgmq orders within a **group**, keyed by the `x-pgmq-group` header:

```kotlin
@PgmqPublisher(queue = "points")
fun publish(change: PointsChange, @PgmqGroup userId: String) {}

@PgmqListener(queue = "points", fifo = "true")
fun onChange(change: PointsChange) { ... }
```

Messages of one group are processed in order; different groups run in parallel. `concurrency > 1` is
safe: pgmq skips groups that still have unacknowledged messages, so no second worker can get ahead
inside a group.

Two consequences:

- **A failure stops the rest of its group.** Messages after the failed one are released and
  redelivered after its backoff, rather than running ahead of it.
- **Throughput per group is one batch at a time.** The next batch of a group arrives only after the
  previous one is acknowledged, so `batchSize` is the limiting factor here.

`x-pgmq-group` survives topic fan-out, so ordering guarantees are not lost by routing through a topic.

---

## Topics

pgmq has no topics. This library builds them on top by mapping a topic onto **one real queue per
subscriber** and fanning out on publish.

```kotlin
// every replica receives every message
@PgmqTopicListener(topic = "orders", mode = SubscriptionMode.BROADCAST)
fun notify(order: Order) { ... }

// the replicas share the work, each message handled once
@PgmqTopicListener(topic = "orders", group = "billing")
fun bill(order: Order) { ... }
```

```kotlin
pgmq.topics().publish("orders", order, label = "OrderCreated")
```

| Mode | Queues | Semantics |
|---|---|---|
| `SHARED` (default) | one per group | Each message processed once per group — consumer-group semantics. |
| `BROADCAST` | one per instance | Every replica receives every message. |

`group` defaults to `quarkus.application.name`.

The fan-out runs in **one transaction**: either every subscriber receives the message or none does. A
partial fan-out would leave subscribers permanently out of sync with nothing to indicate it.

### What you must know

**A subscriber that is not registered yet misses messages** — like SNS or Redis pub/sub, not like a
durable queue. Subscriptions are created at startup before any listener consumes, so a running
application never misses its own topic's messages. But for `BROADCAST`, whose queue does not outlive
its instance, anything published while the pod was down is gone.

**Broadcast queues are per instance and need cleaning up.** A graceful shutdown unsubscribes and drops
the queue. A crash cannot, so a janitor reclaims queues whose heartbeat went stale. Both run
automatically; the pairing matters, because a janitor without a heartbeat would reclaim the queues of
healthy instances. `stale-after` must be at least three times `heartbeat-interval`, validated at
startup.

Outside Kubernetes the instance id is a fresh UUID per start, so every restart leaves an orphan for
the janitor. Under Kubernetes `HOSTNAME` is the pod name and the queue is reused.

Using the core API directly:

```kotlin
val topics = template.topics()
topics.initialiseSchema()                                          // once at startup
topics.subscribe("orders", group = "billing")                      // SHARED
topics.subscribe("orders", "notifier", SubscriptionMode.BROADCAST) // per instance
topics.publish("orders", order)
topics.heartbeat("orders", subscriber)                             // periodically
topics.reapStaleSubscriptions()                                    // periodically
```

---

## Several datasources

A client is a named Quarkus datasource:

```properties
quarkus.datasource.jdbc.url=jdbc:postgresql://.../main
quarkus.datasource.analytics.jdbc.url=jdbc:postgresql://.../analytics
```

```kotlin
@PgmqListener(queue = "events", client = "analytics")
fun onEvent(event: EventDto) { ... }

@PgmqPublisher(queue = "events", client = "analytics")
fun publish(event: EventDto) {}

@Inject @PgmqClient("analytics") lateinit var analytics: PgmqTemplate
```

An unconfigured client name fails at startup naming the missing property, rather than silently using
the default datasource. The client also becomes part of the listener id (`analytics/events`), so the
same queue name on two databases stays distinguishable.

---

## Permissions

Per-queue rights, so one role publishes and another consumes. This works because every pgmq function
is `SECURITY INVOKER`, so ordinary table grants decide what a caller may do — no fork required.

```kotlin
pgmq.permissions().grant("orders", role = "publisher", PgmqPrivilege.ENQUEUE)
pgmq.permissions().grant("orders", role = "worker", PgmqPrivilege.DEQUEUE, PgmqPrivilege.ARCHIVE)
pgmq.permissions().effective("orders", "publisher")   // read back from the catalog
pgmq.permissions().revoke("orders", "publisher", PgmqPrivilege.ENQUEUE)
```

| Privilege | Grants |
|---|---|
| `ENQUEUE` | `send`, `send_batch` |
| `DEQUEUE` | `read`, `pop`, `delete`, `set_vt` |
| `ARCHIVE` | moving messages to the archive table |
| `PURGE` | `purge_queue` |
| `MONITOR` | `metrics`, reading queue contents, listing queues |
| `ADMIN` | everything |

Three Postgres details that shape this:

- **`ENQUEUE` also grants `SELECT`.** `pgmq.send` ends in `INSERT … RETURNING msg_id`, and Postgres
  requires `SELECT` on any returned column. A publisher can therefore read the queue table, though it
  still cannot consume without `UPDATE` and `DELETE`.
- **`DEQUEUE` needs `UPDATE`.** `pgmq.read` writes `vt`, `read_ct` and `last_read_at` back.
- **`pgmq.meta` is only granted to `MONITOR` and `ADMIN`.** Granting it broadly would let every
  publisher list every queue in the database, and `send`/`read`/`archive` do not need it.

---

## Controlling listeners at runtime

```kotlin
@Inject lateinit var listeners: PgmqListenerRegistrar

listeners.listeners()        // id, queue, handlers, state, statistics
listeners.stop("mails")      // stops reading; messages stay in the queue
listeners.start("mails")
listeners.pause("mails")     // keeps the threads, stops reading
listeners.resume("mails")
```

The id is `<queue>`, or `<client>/<queue>` for a named client.

**The controllable unit is the queue, not the handler method.** Stopping one of two labelled handlers
would leave the consumer reading messages it can no longer route — they would pile up or be dead
lettered while everything else drains. A queue is either on or off.

Registering at runtime:

```kotlin
listeners.register(
    ListenerSpec(queue = "adhoc", pollInterval = 1.seconds),
    pgmqPayloadHandler<Task> { process(it) },
)
```

Starting disabled:

```kotlin
@PgmqListener(queue = "maintenance", autoStart = "false")
fun onMaintenance(task: Task) { ... }
```

The container exists and shows up as `NOT_STARTED` but reads nothing until `start(id)`.

**Listeners start only after the application is fully up.** The startup observer runs last, so schema
migrations, cache warm-up and your own initialisation have all finished before the first message is
processed.

---

## Metrics

Off by default, since they are only useful with a Micrometer registry present:

```properties
pgmq.metrics.enabled=true
```

```kotlin
implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
```

Turning them on without a registry fails at startup rather than silently recording nothing — an
empty dashboard with no explanation is the worse outcome.

### What you get

| Meter | Type | Tags |
|---|---|---|
| `pgmq.message.processing` | Timer | `queue`, `label`, `outcome` |
| `pgmq.messages.dead.lettered` | Counter | `queue`, `reason` |
| `pgmq.messages.published` | Counter | `queue` |
| `pgmq.queue.length` | Gauge | `queue`, `role` |
| `pgmq.queue.oldest.message.age.seconds` | Gauge | `queue`, `role` |
| `pgmq.listener.inflight` | Gauge | `queue` |

`outcome` is `success`, `retried`, `dead_lettered` or `unroutable`. `reason` is `malformed`,
`unroutable`, `schema_version`, `permanent_failure` or `retries_exhausted`. `role` is `main` or
`dlq`, so one query covers a queue and its dead letter queue without knowing the naming convention.

There is no separate counter for processed messages: a Micrometer timer already carries its own
count, so `pgmq_message_processing_seconds_count` is the throughput and the quantiles of the same
meter are the latency.

### The two that matter most

**`pgmq.message.processing` against your visibility timeout.** This is the only way to see the
duplicate-processing trap coming instead of reconstructing it afterwards from duplicates:

```promql
histogram_quantile(0.99, rate(pgmq_message_processing_seconds_bucket[5m]))
```

If that approaches the visibility timeout of the queue, messages will start being redelivered while
they are still being handled. Raise the timeout or turn on `vtRefresh` before it happens.

**`pgmq.queue.oldest.message.age.seconds` for alerting.** Queue length says little on its own — a
thousand messages drained at speed are fine, three stuck for ten minutes are not:

```promql
max by (queue) (pgmq_queue_oldest_message_age_seconds{role="main"}) > 300
```

A dead letter queue that is growing at all is worth an alert of its own:

```promql
increase(pgmq_messages_dead_lettered_total[15m]) > 0
```

### Cost

Everything except the gauges is recorded in the handler path and costs nothing measurable.

The backlog gauges are the exception: `pgmq.metrics()` does a `count(*)` per queue, which grows
expensive on exactly the backlogged queue whose depth you most want to know. They are therefore
filled by a background poller and merely read by the scrape, so a second Prometheus server does not
double the database load:

```properties
pgmq.metrics.queue-gauges=false          # drop the gauges, keep everything else
pgmq.metrics.queue-poll-interval=PT1M    # or just poll less often
```

### Cardinality

`label` is the name of the **handler** that ran, never the label of the incoming message. A label set
per call — `@PgmqLabel` on a publisher parameter — would otherwise create a new time series per
distinct value and eventually take the metrics backend down with it. Messages that reached no handler
are recorded as `label="unrouted"`.

Queue names are bounded in the same way, with one exception worth knowing: a `BROADCAST` topic
listener creates a queue per instance, so its `queue` tag changes on every pod restart. On a cluster
that redeploys often, drop that tag or exclude those queues at the scrape.

### In batch mode

With `batch = "true"` the batch is the unit of work, so `pgmq.message.processing` records **one
observation per batch** rather than one per message. The count is batches, not messages.

---

## Configuration reference

Application-wide:

| Property | Default | |
|---|---|---|
| `pgmq.source-id` | `quarkus.application.name` | Envelope `sourceId`. |
| `pgmq.instance-id` | `$HOSTNAME` or a UUID | Identity for broadcast subscriptions. |
| `pgmq.listeners-enabled` | `true` | Set `false` for a publish-only deployment. |
| `pgmq.verify-extension` | `true` | Checks the installed pgmq version at startup. |
| `pgmq.shutdown-timeout` | `PT30S` | Wait for an in-flight message before giving up. |
| `pgmq.non-retryable` | — | Exception class names treated as permanent. |
| `pgmq.topics.heartbeat-interval` | `PT1M` | |
| `pgmq.topics.stale-after` | `PT15M` | Must be ≥ 3 × heartbeat interval. |
| `pgmq.topics.maintenance-enabled` | `true` | |
| `pgmq.metrics.enabled` | `false` | Publish Micrometer metrics. Needs a registry. |
| `pgmq.metrics.queue-gauges` | `true` | The backlog gauges, the only metrics that query the database. |
| `pgmq.metrics.queue-poll-interval` | `PT30S` | How often those gauges are refreshed. |

Per listener, keyed by id — **these override the annotation**, so throughput can be tuned per
environment without rebuilding:

```properties
pgmq.listener.mails.concurrency=4
pgmq.listener.mails.message-interval=250ms
pgmq.listener.mails.poll-interval=10s
pgmq.listener.mails.visibility-timeout=2m
pgmq.listener.mails.batch-size=5
pgmq.listener.mails.max-retries=3
pgmq.listener.mails.dead-letter-queue=mails_failed
pgmq.listener.mails.vt-refresh=true
pgmq.listener.mails.auto-start=false
```

Per publisher, keyed by the method name in kebab-case — these **fill in blanks only**:

```properties
pgmq.publisher.send-urgent.target-id=mailer-eu
pgmq.publisher.send-urgent.delay=5s
```

---

## Common mistakes

**Putting logic in a `@PgmqPublisher` body.** It never runs. Validation, logging, metrics — all
silently skipped. Do that work in the caller.

**Assuming replicas prevent duplicates.** `FOR UPDATE SKIP LOCKED` stops two consumers from holding a
message at the same time. It does not stop redelivery after a timeout expires. Design handlers to be
idempotent.

**Setting `visibilityTimeout` too low for a throttled listener.** The library rejects the impossible
combinations, but if you set a value that merely *usually* fits, you get duplicates under load. Prefer
leaving it derived, or enable `vtRefresh`.

**Expecting `targetId` to route.** It is an assertion for the consumer to check, not a filter. Use
separate queues or a topic.

**Expecting configuration to override a publisher annotation.** It does not — that direction is
reserved for listeners. Use a `${...}` expression in the annotation.

**Treating a topic as durable.** A subscriber that has not registered yet misses messages. A
`BROADCAST` subscriber that was down misses everything published meanwhile.

**Different settings for two handlers on the same queue.** The build fails, on purpose. Move one
handler to its own queue if it really needs different throughput.

**Relying on library logs without the SLF4J bridge.** In Quarkus it ships with the extension. In a
plain JVM application, add an SLF4J binding or every warning is discarded.

---

## What this library does not do

- **Exactly-once delivery.** Nothing can, for handlers with external side effects. If your handler
  only writes to the same Postgres, put the work and the acknowledgement in one transaction and you
  get exactly-once in effect. Otherwise pass `messageId` to the receiving system as its idempotency
  key — most providers support one.
- **Queues in other schemas.** pgmq hardcodes the `pgmq` schema. Use a naming convention, or separate
  datasources for real isolation.
- **Push delivery.** `pgmq.enable_notify_insert` is exposed on the template, but listeners poll on
  purpose so you keep control of the processing rate.
- **`suspend` handlers.** Blocking JDBC means they would buy nothing for queue operations.

---

## Native image

Supported and tested. Handler invocation goes through reflection, so the extension registers the
listener beans, payload types and publisher payloads for reflective access, declares the dynamic
proxy behind every interface publisher, and forces the UUID generator to initialise at runtime — otherwise the image would bake in a fixed random seed and every
instance would generate identical `messageId` values.

```bash
./gradlew build -Dquarkus.native.enabled=true -Dquarkus.native.container-build=true
```

If you take several parameters in a `@PgmqPublisher` method, compile with `-parameters` (Java) or
`-java-parameters` (Kotlin) so the payload field names survive. Without them the call fails with an
explicit error rather than producing a payload with wrong field names.

---

## Contributing and releasing

Every pull request and every push to `main` runs the full test suite against a real PGMQ container.
There are no mocks in this repository on purpose: the behaviour that matters — row locking, visibility
timeouts, `read_grouped` isolation — cannot be reproduced by a fake.

Running the tests locally needs a PGMQ on hand:

```bash
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:v1.10.0
```

Then `CREATE EXTENSION pgmq;` once, and `./gradlew build`. Point the tests elsewhere with
`-Ppgmq.test.jdbc-url=...`, `-Ppgmq.test.user=...`, `-Ppgmq.test.password=...`.

A release is the **Release** workflow, run manually with a version number. It validates the version,
runs the tests against it, publishes all three modules to Maven Central, and only then creates the
GitHub release — so a failed upload never leaves a release tag pointing at artifacts that do not
exist. Versions on Central are immutable, which is why the workflow refuses a tag that already exists.

Four repository secrets are required:

| Secret | |
|---|---|
| `MAVEN_CENTRAL_USERNAME` | Central Portal token name |
| `MAVEN_CENTRAL_PASSWORD` | Central Portal token value |
| `SIGNING_KEY` | GPG private key, ASCII-armoured (`gpg --armor --export-secret-keys <id>`) |
| `SIGNING_KEY_PASSWORD` | its passphrase |

The public half of the signing key must be on a public keyserver, or Central rejects the signatures.

---

## License

[Apache License 2.0](LICENSE). Use it in commercial projects, fork it, ship it — the only obligations
are to keep the licence and notices, and to state what you changed. Apache 2.0 rather than MIT for its
explicit patent grant, which protects both you and any contributor.
