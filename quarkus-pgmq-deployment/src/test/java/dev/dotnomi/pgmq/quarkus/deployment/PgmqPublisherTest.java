package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqMessage;
import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.quarkus.PgmqDelay;
import dev.dotnomi.pgmq.quarkus.PgmqHeader;
import dev.dotnomi.pgmq.quarkus.PgmqLabel;
import dev.dotnomi.pgmq.quarkus.PgmqMessageCustomizer;
import dev.dotnomi.pgmq.quarkus.PgmqPublisher;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the {@code @PgmqPublisher} interceptor actually publishes: the method body never runs, the
 * declarative defaults land on the envelope, annotated parameters stay out of the payload, and the
 * customizer wins over everything else.
 */
public class PgmqPublisherTest {
    static final String QUEUE = "ext_publisher_test";

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
        .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(Publishers.class, Mail.class))
        .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
        .overrideConfigKey("quarkus.datasource.jdbc.url", System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
        .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
        .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"))
        // Nothing consumes this queue; the test reads it directly.
        .overrideConfigKey("pgmq.publisher.send-configured.label", "from-config");

    @Inject
    Publishers publishers;

    @Inject
    PgmqTemplate template;

    @BeforeEach
    void resetQueue() {
        template.createQueueIfMissing(QUEUE);
        template.purgeQueue(QUEUE);
    }

    private PgmqMessage<String> readOne() {
        List<PgmqMessage<String>> messages = template.readRaw(QUEUE);
        assertThat(messages).hasSize(1);
        return messages.get(0);
    }

    @Test
    void singleParameterBecomesThePayloadAndTypeDefaultsApply() {
        publishers.sendMail(new Mail("a@b.de", "hello"));

        var message = readOne();
        assertThat(message.getPayload()).contains("a@b.de").contains("hello");
        assertThat(message.getEnvelope()).isNotNull();
        assertThat(message.getEnvelope().getTargetId())
                .describedAs("type-level default applies")
                .isEqualTo("mailer");
        assertThat(message.getEnvelope().getMessageId()).isNotBlank();
    }

    @Test
    void severalParametersBecomeAJsonObjectKeyedByParameterName() {
        publishers.sendParts("c@d.de", "body text");

        var message = readOne();
        assertThat(message.getPayload())
            .contains("recipient").contains("c@d.de")
            .contains("text").contains("body text");
    }

    @Test
    void methodAnnotationOverridesTypeAndAnnotatedParametersStayOutOfThePayload() {
        long msgId = publishers.sendUrgent(new Mail("e@f.de", "urgent"), 9);
        assertThat(msgId).isPositive();

        var message = readOne();
        assertThat(message.getEnvelope()).isNotNull();
        assertThat(message.getEnvelope().getLabel()).isEqualTo("Urgent");
        assertThat(message.getEnvelope().getSchemaVersion()).isEqualTo(2);
        assertThat(message.getHeaders()).containsEntry("priority", "9");
        assertThat(message.getPayload())
            .describedAs("an @PgmqHeader parameter must not leak into the payload")
            .doesNotContain("priority");
    }

    @Test
    void parameterAnnotationsOverrideTheMethodAnnotation() {
        publishers.sendWithOverrides(new Mail("g@h.de", "x"), "FromParameter");

        var message = readOne();
        assertThat(message.getEnvelope()).isNotNull();
        assertThat(message.getEnvelope().getLabel())
            .describedAs("@PgmqLabel parameter beats the method attribute")
            .isEqualTo("FromParameter");
    }

    @Test
    void customizerWinsOverEverythingElse() {
        publishers.sendCustomizable(
            new Mail("i@j.de", "x"),
            builder -> builder.label("FromCustomizer").header("extra", "yes")
        );

        var message = readOne();
        assertThat(message.getEnvelope()).isNotNull();
        assertThat(message.getEnvelope().getLabel()).isEqualTo("FromCustomizer");
        assertThat(message.getHeaders()).containsEntry("extra", "yes");
        assertThat(message.getPayload())
            .describedAs("the customizer parameter is recognised by type and never becomes payload")
            .doesNotContain("FromCustomizer");
    }

    @Test
    void configurationOverridesAreApplied() {
        publishers.sendConfigured(new Mail("k@l.de", "x"));
        var message = readOne();
        assertThat(message.getEnvelope()).isNotNull();
        assertThat(message.getEnvelope().getLabel())
            .describedAs(
                "configuration fills what the annotations leave blank; an explicit "
                + "annotation value still wins, which is what ${config.key} expressions are for"
            )
            .isEqualTo("from-config");
    }

    @Test
    void delayParameterKeepsTheMessageInvisible() {
        publishers.sendDelayed(new Mail("m@n.de", "x"), 60L);

        assertThat(template.readRaw(QUEUE)).isEmpty();
        assertThat(template.metrics(QUEUE).getLength()).isEqualTo(1);
    }

    public record Mail(String recipient, String text) {
    }

    @ApplicationScoped
    @PgmqPublisher(queue = QUEUE, targetId = "mailer")
    public static class Publishers {
        // Bodies below are never executed — the interceptor publishes instead.

        public void sendMail(Mail mail) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }

        public void sendParts(String recipient, String text) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }

        @PgmqPublisher(label = "Urgent", schemaVersion = "2")
        public long sendUrgent(Mail mail, @PgmqHeader("priority") int priority) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }

        @PgmqPublisher(label = "FromMethod")
        public void sendWithOverrides(Mail mail, @PgmqLabel String label) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }

        @PgmqPublisher(label = "FromMethod")
        public void sendCustomizable(Mail mail, PgmqMessageCustomizer customizer) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }

        public void sendConfigured(Mail mail) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }

        public void sendDelayed(Mail mail, @PgmqDelay long delaySeconds) {
            throw new AssertionError("the body of a @PgmqPublisher method must not run");
        }
    }
}
