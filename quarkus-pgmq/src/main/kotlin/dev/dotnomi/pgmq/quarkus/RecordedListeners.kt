package dev.dotnomi.pgmq.quarkus

import jakarta.inject.Singleton
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Holds build-time listener metadata until startup is complete.
 *
 * The recorder runs at `RUNTIME_INIT`, before the datasource exists, so it can only deposit metadata
 * here; [PgmqLifecycle] turns it into containers later.
 */
// @Singleton, not @ApplicationScoped: at RUNTIME_INIT a client proxy would have nothing to delegate to.
@Singleton
class RecordedListeners {
    private val entries = CopyOnWriteArrayList<RecordedListener>()

    fun addAll(listeners: Collection<RecordedListener>) {
        entries.addAll(listeners)
    }

    fun all(): List<RecordedListener> = entries.toList()

    fun topicListeners(): List<RecordedListener> = entries.filter { it.topic.isNotBlank() }
}
