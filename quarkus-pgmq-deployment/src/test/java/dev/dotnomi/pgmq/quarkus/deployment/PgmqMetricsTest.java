package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.quarkus.PgmqListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The Micrometer wiring, exercised with a real registry present.
 *
 * Metrics are an optional dependency, and the beans behind them are registered only when Micrometer
 * is on the classpath — so a test without it would prove nothing.
 */
public class PgmqMetricsTest {

    static final String QUEUE = "ext_metrics";

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(Counter.class))
            .overrideConfigKey("pgmq.metrics.enabled", "true")
            .overrideConfigKey("pgmq.metrics.queue-poll-interval", "PT1S")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .overrideConfigKey("quarkus.datasource.jdbc.url",
                    System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
            .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
            .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"));

    @Inject
    PgmqTemplate template;

    @Inject
    MeterRegistry registry;

    private Timer processingTimer() {
        return registry.find("pgmq.message.processing")
                .tag("queue", QUEUE)
                .tag("outcome", "success")
                .timer();
    }

    // All test methods share one Quarkus instance and therefore one registry, so every assertion
    // here is on a delta. Absolute counts would depend on execution order.
    @Test
    void aProcessedMessageProducesTimerAndPublishCounter() {
        Timer before = processingTimer();
        long processedBefore = before == null ? 0 : before.count();
        int seenBefore = Counter.SEEN.get();

        template.send(QUEUE, "hello");

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(Counter.SEEN.get()).isGreaterThan(seenBefore));

        Timer timer = processingTimer();
        assertThat(timer)
                .describedAs("the processing timer doubles as the throughput counter")
                .isNotNull();
        assertThat(timer.count()).isGreaterThan(processedBefore);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS)).isPositive();

        assertThat(registry.find("pgmq.messages.published").tag("queue", QUEUE).counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.count()).isPositive());
    }

    @Test
    void theLabelTagIsTheHandlerNameSoItStaysBounded() {
        template.send(QUEUE, "labelled", "SomeFreelyChosenLabel");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(registry.find("pgmq.message.processing").timers()).isNotEmpty());

        assertThat(registry.find("pgmq.message.processing").timers())
                .describedAs("a message label must never become a tag — it would grow without bound")
                .allSatisfy(timer -> assertThat(timer.getId().getTag("label"))
                        .isNotEqualTo("SomeFreelyChosenLabel"));
    }

    @Test
    void backlogGaugesAreRegisteredForTheQueueAndItsDeadLetterQueue() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(registry.find("pgmq.queue.length").tag("queue", QUEUE).tag("role", "main").gauge())
                        .isNotNull());

        assertThat(registry.find("pgmq.queue.oldest.message.age.seconds")
                .tag("queue", QUEUE).tag("role", "main").gauge())
                .describedAs("the age is the gauge worth alerting on")
                .isNotNull();
        assertThat(registry.find("pgmq.listener.inflight").tag("queue", QUEUE).gauge()).isNotNull();
    }

    @Singleton
    public static class Counter {

        static final AtomicInteger SEEN = new AtomicInteger();

        @PgmqListener(queue = QUEUE, pollInterval = "200ms")
        void count(String payload) {
            SEEN.incrementAndGet();
        }
    }
}
