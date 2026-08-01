package dev.dotnomi.pgmq.internal

import java.sql.Connection
import javax.sql.DataSource

/** Where a template takes its connections from. */
internal interface ConnectionSource {
    fun <R> use(block: (Connection) -> R): R

    /**
     * Whether each operation is its own short transaction.
     *
     * Matters for FIFO reads: `pgmq.read_grouped` holds a queue-wide lock until the transaction ends.
     */
    val isAutoCommitScoped: Boolean
}

internal class DataSourceConnectionSource(private val dataSource: DataSource) : ConnectionSource {
    override fun <R> use(block: (Connection) -> R): R = dataSource.connection.use(block)
    override val isAutoCommitScoped: Boolean get() = true
}

/** Binds a template to an existing connection without closing it. */
internal class FixedConnectionSource(private val connection: Connection) : ConnectionSource {
    override fun <R> use(block: (Connection) -> R): R = block(connection)
    override val isAutoCommitScoped: Boolean get() = connection.autoCommit
}
