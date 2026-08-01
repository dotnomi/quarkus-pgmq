package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.quarkus.PgmqPublisher;
import io.quarkus.test.QuarkusUnitTest;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An abstract class is neither instantiable by CDI nor proxyable, so it must fail at build time with
 * a message naming the two forms that do work.
 */
public class PgmqAbstractPublisherTest {

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class).addClasses(HalfBaked.class))
            .assertException(t -> assertThat(t)
                    .hasMessageContaining("is abstract")
                    .hasMessageContaining("declare it as an interface"));

    @Test
    void theBuildFailsBeforeTheTestRuns() {
        Assertions.fail("the build should have failed");
    }

    @PgmqPublisher(queue = "half_baked")
    public abstract static class HalfBaked {

        public abstract void send(String text);
    }
}
