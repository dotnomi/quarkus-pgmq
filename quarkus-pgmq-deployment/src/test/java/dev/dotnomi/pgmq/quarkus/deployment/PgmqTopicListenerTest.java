package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar;
import dev.dotnomi.pgmq.quarkus.PgmqTopicListener;
import dev.dotnomi.pgmq.topics.SubscriptionMode;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Drives {@code @PgmqTopicListener} through a real application: subscribing on startup, resolving
 * the queue that the subscription created, and delivering a published message to the handler.
 */
public class PgmqTopicListenerTest {
    static final String TOPIC = "ext_topic_test";

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
        .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(Consumers.class, Event.class))
        .overrideConfigKey("quarkus.application.name", "topic-app")
        .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
        .overrideConfigKey("quarkus.datasource.jdbc.url", System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
        .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
        .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"));

    @Inject
    PgmqTemplate template;

    @Inject
    PgmqListenerRegistrar registrar;

    @Test
    void topicListenersSubscribeAndReceive() {
        var subscriptions = template.topics().subscriptions(TOPIC);

        // Deliberately not asserting an exact count. The topic outlives a test run, and a BROADCAST
        // subscription from an instance that went away stays registered until the janitor collects
        // it — that is the documented behaviour, not something the test should paper over.
        assertThat(subscriptions).extracting("mode")
            .contains(SubscriptionMode.SHARED, SubscriptionMode.BROADCAST);

        // This *is* deterministic: exactly the two annotated methods of this instance.
        assertThat(registrar.listeners())
            .describedAs("one container per subscription of this instance")
            .hasSize(2);

        registrar.listeners().forEach(l -> template.purgeQueue(l.getQueue()));
        Consumers.shared.clear();
        Consumers.broadcast.clear();

        var written = template.topics().publish(TOPIC, new Event("hello"), "Created");
        assertThat(written).hasSizeGreaterThanOrEqualTo(2);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(Consumers.shared).containsExactly("hello");
            assertThat(Consumers.broadcast).containsExactly("hello");
        });
    }

    @Test
    void groupDefaultsToTheApplicationName() {
        assertThat(template.topics().subscriptions(TOPIC))
            .extracting("group")
            .describedAs("an unset group falls back to quarkus.application.name")
            .contains("topic-app");
    }

    public record Event(String value) { }

    @ApplicationScoped
    public static class Consumers {
        static final List<String> shared = new CopyOnWriteArrayList<>();
        static final List<String> broadcast = new CopyOnWriteArrayList<>();

        /** Group left unset, so it falls back to the application name. */
        @PgmqTopicListener(topic = TOPIC, pollInterval = "200ms")
        void onShared(Event event) {
            shared.add(event.value());
        }

        @PgmqTopicListener(
            topic = TOPIC,
            group = "notifier",
            mode = SubscriptionMode.BROADCAST,
            pollInterval = "200ms"
        )
        void onBroadcast(Event event) {
            broadcast.add(event.value());
        }
    }
}
