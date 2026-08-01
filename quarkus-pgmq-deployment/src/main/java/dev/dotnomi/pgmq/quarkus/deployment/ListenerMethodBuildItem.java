package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.quarkus.RecordedListener;
import io.quarkus.builder.item.MultiBuildItem;

/**
 * One discovered and validated {@code @PgmqListener} method, carried from the scanning build step to
 * the recorder step.
 */
public final class ListenerMethodBuildItem extends MultiBuildItem {
    private final RecordedListener listener;

    public ListenerMethodBuildItem(RecordedListener listener) {
        this.listener = listener;
    }

    public RecordedListener listener() {
        return listener;
    }
}
