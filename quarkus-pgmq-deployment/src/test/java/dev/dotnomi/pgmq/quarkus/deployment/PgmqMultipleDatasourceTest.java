package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.listener.PgmqListenerRegistrar;
import dev.dotnomi.pgmq.quarkus.PgmqClient;
import dev.dotnomi.pgmq.quarkus.PgmqListener;
import dev.dotnomi.pgmq.quarkus.PgmqPublisher;
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
 * Proves that a named client really maps onto a different datasource, rather than quietly falling
 * back to the default one.
 * Both clients point at the same server here but at **different databases**, so a message published
 * through the secondary client is genuinely absent from the primary — which a fallback to the
 * default datasource could never produce.
 */
public class PgmqMultipleDatasourceTest {
    static final String QUEUE = "ext_multi_ds_test";
    private static final String SECONDARY_DB = "pgmq_secondary_test";

    // Runs before the Quarkus instance is built, so the secondary datasource has something to
    // connect to. Static initialisers execute in declaration order, hence the placement.
    static { createSecondaryDatabase(); }

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
        .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(SecondaryPublisher.class, SecondaryConsumer.class, Note.class))
        .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
        .overrideConfigKey("quarkus.datasource.jdbc.url", primaryUrl())
        .overrideConfigKey("quarkus.datasource.username", user())
        .overrideConfigKey("quarkus.datasource.password", password())
        .overrideConfigKey("quarkus.datasource.secondary.db-kind", "postgresql")
        .overrideConfigKey("quarkus.datasource.secondary.jdbc.url", secondaryUrl())
        .overrideConfigKey("quarkus.datasource.secondary.username", user())
        .overrideConfigKey("quarkus.datasource.secondary.password", password());

    static String primaryUrl() {
        return System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres");
    }

    /** Same server, different database — that is what makes the assertion meaningful. */
    static String secondaryUrl() {
        return primaryUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + SECONDARY_DB + "$1");
    }

    static String user() {
        return System.getProperty("pgmq.test.user", "postgres");
    }

    static String password() {
        return System.getProperty("pgmq.test.password", "postgres");
    }

    private static void createSecondaryDatabase() {
        try (
            var admin = java.sql.DriverManager.getConnection(primaryUrl(), user(), password());
            var statement = admin.createStatement()
        ) {
            try (
                var rs = statement.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + SECONDARY_DB + "'")
            ) {
                if (!rs.next()) {
                    // CREATE DATABASE cannot run inside a transaction block.
                    statement.execute("CREATE DATABASE " + SECONDARY_DB);
                }
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Could not prepare the secondary test database", exception);
        }

        try (
            var secondaryConn = java.sql.DriverManager.getConnection(secondaryUrl(), user(), password());
            var statement = secondaryConn.createStatement()
        ) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pgmq");
        } catch (Exception exception) {
            throw new IllegalStateException("Could not install pgmq in the secondary test database", exception);
        }
    }

    @Inject
    PgmqTemplate primary;

    @Inject
    @PgmqClient("secondary")
    PgmqTemplate secondary;

    @Inject
    PgmqListenerRegistrar registrar;

    @Inject
    SecondaryPublisher publisher;

    @Test
    void namedClientInjectionUsesADifferentDatabase() {
        assertThat(secondary).isNotSameAs(primary);

        primary.createQueueIfMissing(QUEUE);
        primary.purgeQueue(QUEUE);
        secondary.purgeQueue(QUEUE);

        secondary.send(QUEUE, new Note("only-in-secondary"));

        assertThat(secondary.metrics(QUEUE).getLength()).isEqualTo(1);
        assertThat(primary.metrics(QUEUE).getLength())
            .describedAs("a fallback to the default datasource would put the message here")
            .isZero();
    }

    @Test
    void publisherWithClientAttributeWritesToThatDatasource() {
        primary.createQueueIfMissing(QUEUE);
        primary.purgeQueue(QUEUE);
        secondary.purgeQueue(QUEUE);
        SecondaryConsumer.received.clear();

        publisher.publishNote(new Note("routed"));

        assertThat(primary.metrics(QUEUE).getLength()).isZero();
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() ->
            assertThat(SecondaryConsumer.received).containsExactly("routed")
        );
    }

    @Test
    void listenerIdCarriesTheClientName() {
        assertThat(registrar.listeners())
            .extracting("id")
            .describedAs("the same queue name on two databases must stay distinguishable")
            .contains("secondary/" + QUEUE);
    }

    public record Note(String text) {
    }

    @ApplicationScoped
    @PgmqPublisher(queue = QUEUE, client = "secondary")
    public static class SecondaryPublisher {
        public void publishNote(Note note) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }
    }

    @ApplicationScoped
    public static class SecondaryConsumer {
        static final List<String> received = new CopyOnWriteArrayList<>();

        @PgmqListener(queue = QUEUE, client = "secondary", pollInterval = "200ms")
        void onNote(Note note) {
            received.add(note.text());
        }
    }
}
