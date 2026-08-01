package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.quarkus.PgmqListener;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.assertj.core.api.Assertions;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code raw = "true"} on an annotated listener, for messages that are only routed or forwarded and
 * would gain nothing from being parsed on the way through.
 */
public class PgmqRawListenerTest {

    static final String QUEUE = "ext_raw_listener";

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(Router.class))
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .overrideConfigKey("quarkus.datasource.jdbc.url",
                    System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
            .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
            .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"));

    @Inject
    PgmqTemplate template;

    @Test
    void aRawListenerSeesTheStoredJsonTextWithoutDeserialising() {
        // A shape no DTO in this application knows about — the whole point of routing raw.
        template.send(QUEUE, Map.of("anything", Map.of("nested", 1), "unexpected", true));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(Router.SEEN).hasSize(1));

        Assertions.assertThat(Router.SEEN.peek())
                .describedAs("the JSON text as stored, not a parsed object")
                .contains("\"anything\"")
                .contains("\"nested\"")
                .contains("\"unexpected\"");
    }

    @Singleton
    public static class Router {

        static final ConcurrentLinkedQueue<String> SEEN = new ConcurrentLinkedQueue<>();

        @PgmqListener(queue = QUEUE, raw = "true", pollInterval = "200ms")
        void route(String json) {
            SEEN.add(json);
        }
    }
}
