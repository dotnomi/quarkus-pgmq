package dev.dotnomi.pgmq.quarkus

import dev.dotnomi.pgmq.topics.SubscriptionMode
import dev.dotnomi.pgmq.topics.TopicSubscription
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.time.toKotlinDuration

/**
 * The topic subscriptions this instance created, with the client they live on.
 *
 * The heartbeat has to name the exact subscriber rows this process owns, which for a
 * [SubscriptionMode.BROADCAST] subscription includes the instance id.
 */
@Singleton
class PgmqOwnedSubscriptions {
    private val entries = CopyOnWriteArrayList<Owned>()

    fun record(client: String, subscription: TopicSubscription) {
        entries += Owned(client, subscription)
    }

    fun all(): List<Owned> = entries.toList()

    data class Owned(val client: String, val subscription: TopicSubscription)
}

/**
 * Keeps topic subscriptions alive and reclaims the ones whose owner is gone.
 *
 * The two halves are useless apart: without the heartbeat the janitor would reclaim healthy
 * instances' queues. Every instance runs the janitor; an advisory lock makes only one do the work.
 */
@ApplicationScoped
class PgmqTopicMaintenance(
    private val templates: PgmqTemplateRegistry,
    private val owned: PgmqOwnedSubscriptions,

    /** How often each owned subscription refreshes its heartbeat. */
    @param:ConfigProperty(name = "pgmq.topics.heartbeat-interval", defaultValue = "PT1M")
    private val heartbeatInterval: java.time.Duration,

    /** How long without a heartbeat before a subscription counts as dead. */
    @param:ConfigProperty(name = "pgmq.topics.stale-after", defaultValue = "PT15M")
    private val staleAfter: java.time.Duration,

    @param:ConfigProperty(name = "pgmq.topics.maintenance-enabled", defaultValue = "true")
    private val enabled: Boolean,
) {
    private val log = LoggerFactory.getLogger(PgmqTopicMaintenance::class.java)

    @Volatile
    private var executor: ScheduledExecutorService? = null

    fun start() {
        if (!enabled) {
            log.info("pgmq.topics.maintenance-enabled=false — no heartbeat and no janitor will run.")
            return
        }
        if (owned.all().isEmpty()) {
            log.debug("No topic subscriptions owned by this instance; maintenance not started.")
            return
        }

        require(staleAfter > heartbeatInterval.multipliedBy(MINIMUM_STALE_FACTOR)) {
            "pgmq.topics.stale-after ($staleAfter) must be at least $MINIMUM_STALE_FACTOR times " +
                "pgmq.topics.heartbeat-interval ($heartbeatInterval). Otherwise a single missed " +
                "heartbeat would make the janitor reclaim the queue of a healthy instance."
        }

        val pool = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "pgmq-topic-maintenance").apply { isDaemon = true }
        }
        executor = pool

        val periodMillis = heartbeatInterval.toMillis()
        pool.scheduleWithFixedDelay(
            ::runOnce,
            periodMillis,
            periodMillis,
            TimeUnit.MILLISECONDS,
        )

        log.info(
            "Topic maintenance started: heartbeat every {}, reclaiming subscriptions stale for {}.",
            heartbeatInterval, staleAfter,
        )
    }

    fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    /** Runs one cycle without waiting for the schedule. */
    fun runOnce() {
        runCatching { sendHeartbeats() }
            .onFailure { log.warn("Topic heartbeat failed; will retry on the next cycle.", it) }
        runCatching { reap() }
            .onFailure { log.warn("Topic janitor failed; will retry on the next cycle.", it) }
    }

    private fun sendHeartbeats() {
        owned.all().forEach { (client, subscription) ->
            templates.template(client)
                .topics()
                .heartbeat(subscription.topic, subscription.subscriber)
        }
    }

    private fun reap() {
        // One pass per client: different datasources are different databases.
        owned.all().map { it.client }.distinct().forEach { client ->
            val reaped = templates.template(client)
                .topics()
                .reapStaleSubscriptions(staleAfter.toKotlinDuration())
            if (reaped.isNotEmpty()) {
                log.info("Reclaimed {} stale topic queue(s) on client '{}': {}", reaped.size, client, reaped)
            }
        }
    }

    private companion object {
        /** Tolerates a few missed cycles before declaring an instance dead. */
        const val MINIMUM_STALE_FACTOR = 3L
    }
}
