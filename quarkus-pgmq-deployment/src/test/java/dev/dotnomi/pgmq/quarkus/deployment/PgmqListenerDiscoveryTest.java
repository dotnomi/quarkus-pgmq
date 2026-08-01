package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.listener.ListenerState;
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar;
import dev.dotnomi.pgmq.quarkus.PgmqListener;
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
 * Boots a real Quarkus application with the extension installed and proves the whole chain works:
 * the annotated method is discovered at build time, a container is created for it, and a published
 * message actually reaches the handler.
 */
public class PgmqListenerDiscoveryTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
        .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(Consumers.class, Payload.class))
        .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
        .overrideConfigKey("quarkus.datasource.jdbc.url", System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
        .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
        .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"));

    @Inject
    PgmqListenerRegistrar registrar;

    @Inject
    PgmqTemplate template;

    @Test
    void annotatedMethodsBecomeContainersAndReceiveMessages() {
        // Two labelled handlers on one queue must end up in a single container.
        var listeners = registrar.listeners();
        assertThat(listeners).hasSize(1);

        var info = listeners.get(0);
        assertThat(info.getQueue()).isEqualTo(Consumers.QUEUE);
        assertThat(info.getHandlers())
            .describedAs("both labels share one container")
            .hasSize(2);

        // The queue name has to be a compile-time constant for the annotation, so it survives
        // between runs. Start from a known-empty state instead of inheriting leftovers.
        String id = info.getId();
        registrar.stop(id);
        template.purgeQueue(Consumers.QUEUE);
        template.purgeQueue(Consumers.QUEUE + "_dlq");
        Consumers.created.clear();
        Consumers.cancelled.clear();
        registrar.start(id);
        assertThat(registrar.state(id)).isEqualTo(ListenerState.RUNNING);

        template.send(Consumers.QUEUE, new Payload("first"), "Created");
        template.send(Consumers.QUEUE, new Payload("second"), "Cancelled");

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(Consumers.created).containsExactly("first");
            assertThat(Consumers.cancelled).containsExactly("second");
        });
    }

    @Test
    void listenersCanBeStoppedAndStartedById() {
        String id = registrar.listeners().get(0).getId();
        assertThat(id).isEqualTo(Consumers.QUEUE);

        registrar.stop(id);
        assertThat(registrar.state(id)).isEqualTo(ListenerState.STOPPED);

        registrar.start(id);
        assertThat(registrar.isRunning(id)).isTrue();
    }

    public record Payload(String value) {
    }

    @ApplicationScoped
    public static class Consumers {
        static final String QUEUE = "ext_discovery_test";
        static final List<String> created = new CopyOnWriteArrayList<>();
        static final List<String> cancelled = new CopyOnWriteArrayList<>();

        @PgmqListener(queue = QUEUE, label = "Created", pollInterval = "200ms")
        void onCreated(Payload payload) {
            created.add(payload.value());
        }

        @PgmqListener(queue = QUEUE, label = "Cancelled", pollInterval = "200ms")
        void onCancelled(Payload payload) {
            cancelled.add(payload.value());
        }
    }
}
