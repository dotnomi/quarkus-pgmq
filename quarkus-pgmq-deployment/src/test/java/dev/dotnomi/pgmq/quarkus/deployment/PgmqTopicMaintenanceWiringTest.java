package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.quarkus.PgmqOwnedSubscriptions;
import dev.dotnomi.pgmq.quarkus.PgmqTopicListener;
import dev.dotnomi.pgmq.quarkus.PgmqTopicMaintenance;
import dev.dotnomi.pgmq.topics.SubscriptionMode;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regression this whole class exists for: heartbeat and janitor were implemented and unit tested,
 * but nothing in a running application ever called them. Both queue leaks and — worse — reclaiming a
 * healthy instance's queue were therefore possible in production while every test stayed green.
 * This asserts the wiring itself: that a started application knows which subscriptions it owns and
 * that a maintenance cycle leaves them alone.
 */
public class PgmqTopicMaintenanceWiringTest {
    static final String TOPIC = "ext_maintenance_test";

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
        .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(Consumer.class, Ping.class))
        .overrideConfigKey("quarkus.application.name", "maintenance-app")
        .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
        .overrideConfigKey("quarkus.datasource.jdbc.url", System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
        .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
        .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"))
        // Short enough to exercise the validation path without slowing the test down.
        .overrideConfigKey("pgmq.topics.heartbeat-interval", "PT1S")
        .overrideConfigKey("pgmq.topics.stale-after", "PT10S");

    @Inject
    PgmqOwnedSubscriptions owned;

    @Inject
    PgmqTopicMaintenance maintenance;

    @Inject
    PgmqTemplate template;

    @Test
    void theApplicationKnowsWhichSubscriptionsItOwns() {
        assertThat(owned.all())
            .describedAs("without this the heartbeat cannot name the subscriber rows to refresh")
            .isNotEmpty();

        assertThat(owned.all())
            .extracting(o -> o.getSubscription().getMode())
            .contains(SubscriptionMode.BROADCAST);
    }

    @Test
    void aMaintenanceCycleDoesNotReclaimThisInstance() {
        var mine = owned.all().stream()
            .filter(o -> o.getSubscription().getTopic().equals(TOPIC))
            .findFirst()
            .orElseThrow();
        var queue = mine.getSubscription().getQueue();

        assertThat(template.queueExists(queue)).isTrue();

        // Runs heartbeat and janitor exactly as the scheduled loop does.
        maintenance.runOnce();
        maintenance.runOnce();

        assertThat(template.queueExists(queue))
            .describedAs("a running instance must survive its own janitor")
            .isTrue();
        assertThat(template.topics().subscriptions(TOPIC))
            .extracting("subscriber")
            .contains(mine.getSubscription().getSubscriber());
    }

    public record Ping(String value) { }

    @ApplicationScoped
    public static class Consumer {
        @PgmqTopicListener(topic = TOPIC, mode = SubscriptionMode.BROADCAST, pollInterval = "500ms")
        void onPing(Ping ping) {
            // Not exercised here; this test is about the maintenance wiring.
        }
    }
}
