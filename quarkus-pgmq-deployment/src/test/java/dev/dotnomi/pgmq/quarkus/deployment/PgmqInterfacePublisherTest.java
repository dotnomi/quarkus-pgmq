package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.PgmqMessage;
import dev.dotnomi.pgmq.PgmqTemplate;
import dev.dotnomi.pgmq.quarkus.PgmqHeader;
import dev.dotnomi.pgmq.quarkus.PgmqLabel;
import dev.dotnomi.pgmq.quarkus.PgmqPublisher;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A publisher declared as an interface, so no method body exists to be ignored.
 *
 * An interface cannot be a CDI bean, so this goes through a generated proxy rather than the
 * interceptor. Default methods keep their own behaviour.
 */
public class PgmqInterfacePublisherTest {

    static final String QUEUE = "ext_iface_publisher";

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class)
                    .addClasses(Mails.class, Mail.class))
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .overrideConfigKey("quarkus.datasource.jdbc.url",
                    System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres"))
            .overrideConfigKey("quarkus.datasource.username", System.getProperty("pgmq.test.user", "postgres"))
            .overrideConfigKey("quarkus.datasource.password", System.getProperty("pgmq.test.password", "postgres"));

    @Inject
    Mails mails;

    @Inject
    PgmqTemplate template;

    @BeforeEach
    void reset() {
        template.createQueueIfMissing(QUEUE);
        template.purgeQueue(QUEUE);
    }

    private PgmqMessage<String> readOne() {
        List<PgmqMessage<String>> messages = template.readRaw(QUEUE);
        assertThat(messages).hasSize(1);
        return messages.get(0);
    }

    @Test
    void anInterfaceIsInjectableAndPublishes() {
        assertThat(mails).isNotNull();

        mails.sendMail(new Mail("a@b.de", "hello"));

        var message = readOne();
        assertThat(message.getPayload()).contains("a@b.de");
        assertThat(message.getEnvelope()).isNotNull();
        assertThat(message.getEnvelope().getTargetId()).isEqualTo("mailer");
    }

    @Test
    void parameterAnnotationsWorkTheSameAsOnAClass() {
        long msgId = mails.sendUrgent(new Mail("c@d.de", "urgent"), "Escalation", 9);
        assertThat(msgId).isPositive();

        var message = readOne();
        assertThat(message.getEnvelope().getLabel()).isEqualTo("Escalation");
        assertThat(message.getHeaders()).containsEntry("priority", "9");
        assertThat(message.getPayload()).doesNotContain("priority");
    }

    @Test
    void defaultMethodsKeepTheirOwnBody() {
        mails.sendGreeting("Ada");

        var message = readOne();
        assertThat(message.getPayload())
                .describedAs("the default method built the payload itself and then delegated")
                .contains("Hello Ada");
    }

    @Test
    void objectMethodsBehaveSensibly() {
        assertThat(mails).isEqualTo(mails);
        assertThat(mails.hashCode()).isEqualTo(mails.hashCode());
        assertThat(mails.toString()).contains("Mails");
    }

    public record Mail(String recipient, String text) {
    }

    @PgmqPublisher(queue = QUEUE, targetId = "mailer")
    public interface Mails {

        void sendMail(Mail mail);

        long sendUrgent(Mail mail, @PgmqLabel String label, @PgmqHeader("priority") int priority);

        /** Not abstract, so this body really runs and then publishes through [sendMail]. */
        default void sendGreeting(String name) {
            sendMail(new Mail("greetings@example.com", "Hello " + name));
        }
    }
}
