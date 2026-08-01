package dev.dotnomi.pgmq.permissions

import dev.dotnomi.pgmq.internal.ConnectionSource
import dev.dotnomi.pgmq.internal.Identifiers
import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * Grants and revokes per-queue rights.
 *
 * `GRANT` takes no bind parameters, so queue and role names are interpolated. They go through the
 * strict whitelist in [Identifiers] and are quoted — the one place with tightened name rules.
 */
class PgmqPermissionOperations internal constructor(
    private val connections: ConnectionSource,
) {
    private val log = LoggerFactory.getLogger(PgmqPermissionOperations::class.java)

    /** Grants [privileges] on [queue] to [role]. Additive. */
    fun grant(queue: String, role: String, vararg privileges: PgmqPrivilege) {
        apply(queue, role, privileges.toSet(), granting = true)
    }

    /** Removes [privileges] on [queue] from [role]. */
    fun revoke(queue: String, role: String, vararg privileges: PgmqPrivilege) {
        apply(queue, role, privileges.toSet(), granting = false)
    }

    /** Removes every right [role] holds on [queue]. */
    fun reset(queue: String, role: String) {
        apply(queue, role, PgmqPrivilege.entries.toSet(), granting = false)
    }

    /**
     * What [role] can actually do with [queue], queried from the catalog so inherited grants and
     * grants made elsewhere show up too.
     */
    fun effective(queue: String, role: String): EffectivePermissions = connections.use { conn ->
        val safeRole = Identifiers.requireSafeIdentifier(role, "Role name")
        val tables = resolveTables(conn, queue)

        val raw = buildMap {
            put(tables.queueTable, tablePrivileges(conn, safeRole, tables.queueTable))
            if (tables.archiveTable != null) {
                put(tables.archiveTable, tablePrivileges(conn, safeRole, tables.archiveTable))
            }
        }

        val onQueue = raw[tables.queueTable].orEmpty()
        val onArchive = raw[tables.archiveTable].orEmpty()

        val privileges = buildSet {
            if ("INSERT" in onQueue) add(PgmqPrivilege.ENQUEUE)
            if (setOf("SELECT", "UPDATE", "DELETE").all { it in onQueue }) add(PgmqPrivilege.DEQUEUE)
            if ("INSERT" in onArchive) add(PgmqPrivilege.ARCHIVE)
            if ("DELETE" in onQueue) add(PgmqPrivilege.PURGE)
            if ("SELECT" in onQueue) add(PgmqPrivilege.MONITOR)
            if (setOf("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE").all { it in onQueue }) {
                add(PgmqPrivilege.ADMIN)
            }
        }

        EffectivePermissions(queue = queue, role = safeRole, privileges = privileges, rawGrants = raw)
    }

    private fun apply(queue: String, role: String, privileges: Set<PgmqPrivilege>, granting: Boolean) {
        if (privileges.isEmpty()) return

        val safeRole = Identifiers.requireSafeIdentifier(role, "Role name")
        val quotedRole = Identifiers.quote(safeRole)
        val verb = if (granting) "GRANT" else "REVOKE"
        val preposition = if (granting) "TO" else "FROM"

        connections.use { conn ->
            val tables = resolveTables(conn, queue)
            val statements = mutableListOf<String>()

            // Never revoked: other queues in the same schema would stop working. pgmq.meta is
            // not granted here — send, read and archive do not need it, and it would let every role
            // list every queue.
            if (granting) {
                statements += "GRANT USAGE ON SCHEMA pgmq TO $quotedRole"
            }

            privileges.forEach { privilege ->
                statements += statementsFor(privilege, tables, quotedRole, verb, preposition)
            }

            conn.createStatement().use { statement ->
                statements.forEach {
                    log.debug("{}", it)
                    statement.execute(it)
                }
            }
        }

        log.info(
            "{} {} on queue '{}' {} role '{}'.",
            verb, privileges.sorted(), queue, preposition.lowercase(), safeRole,
        )
    }

    private fun statementsFor(
        privilege: PgmqPrivilege,
        tables: QueueTables,
        quotedRole: String,
        verb: String,
        preposition: String,
    ): List<String> {
        val q = tables.quotedQueueTable
        val a = tables.quotedArchiveTable

        return when (privilege) {
            // SELECT is mandatory: pgmq.send ends in `INSERT … RETURNING msg_id`, and Postgres
            // requires SELECT on returned columns. The sequence grant only applies to serial columns.
            PgmqPrivilege.ENQUEUE -> buildList {
                add("$verb INSERT, SELECT ON $q $preposition $quotedRole")
                tables.quotedSequence?.let { add("$verb USAGE ON SEQUENCE $it $preposition $quotedRole") }
            }

            // pgmq.read writes vt, read_ct and last_read_at back, so UPDATE is mandatory.
            PgmqPrivilege.DEQUEUE -> listOf("$verb SELECT, UPDATE, DELETE ON $q $preposition $quotedRole")

            // Same RETURNING rule as ENQUEUE.
            PgmqPrivilege.ARCHIVE -> buildList {
                if (a != null) add("$verb INSERT, SELECT ON $a $preposition $quotedRole")
                add("$verb SELECT, DELETE ON $q $preposition $quotedRole")
            }

            PgmqPrivilege.PURGE -> listOf("$verb DELETE ON $q $preposition $quotedRole")

            // metrics() reads the sequence's last_value, so the table grant alone is not enough.
            PgmqPrivilege.MONITOR -> buildList {
                add("$verb SELECT ON $q $preposition $quotedRole")
                add("$verb SELECT ON pgmq.meta $preposition $quotedRole")
                tables.quotedSequence?.let { add("$verb SELECT ON SEQUENCE $it $preposition $quotedRole") }
            }

            PgmqPrivilege.ADMIN -> buildList {
                add("$verb ALL PRIVILEGES ON $q $preposition $quotedRole")
                add("$verb SELECT ON pgmq.meta $preposition $quotedRole")
                if (a != null) add("$verb ALL PRIVILEGES ON $a $preposition $quotedRole")
                tables.quotedSequence?.let { add("$verb ALL PRIVILEGES ON SEQUENCE $it $preposition $quotedRole") }
            }
        }
    }

    private class QueueTables(
        val queueTable: String,
        val archiveTable: String?,
        val sequence: String?,
    ) {
        val quotedQueueTable: String get() = "pgmq.${Identifiers.quote(queueTable)}"
        val quotedArchiveTable: String? get() = archiveTable?.let { "pgmq.${Identifiers.quote(it)}" }
        val quotedSequence: String? get() = sequence
    }

    /** Asks pgmq for the physical table names rather than assuming the `q_`/`a_` prefixes. */
    private fun resolveTables(conn: Connection, queue: String): QueueTables {
        Identifiers.requireValidQueueName(queue)

        val queueTable = try {
            conn.prepareStatement("SELECT pgmq.format_table_name(?, 'q')").use { ps ->
                ps.setString(1, queue)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "pgmq.format_table_name returned nothing for queue '$queue'." }
                    rs.getString(1)
                }
            }
        } catch (e: java.sql.SQLException) {
            // pgmq rejects anything it would not accept as a queue name.
            throw IllegalStateException("Queue '$queue' does not exist: ${e.message}", e)
        }

        val archiveTable = conn.prepareStatement("SELECT pgmq.format_table_name(?, 'a')").use { ps ->
            ps.setString(1, queue)
            ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
        }

        // Before the sequence lookup, which raises a raw SQL error on a missing relation.
        check(existsAsTable(conn, queueTable)) {
            "Queue '$queue' does not exist — create it before granting permissions on it."
        }

        // null for identity columns, which need no separate grant.
        val sequence = conn.prepareStatement("SELECT pg_get_serial_sequence(?, 'msg_id')").use { ps ->
            ps.setString(1, "pgmq.${Identifiers.quote(queueTable)}")
            ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
        }

        return QueueTables(
            queueTable = queueTable,
            archiveTable = archiveTable?.takeIf { existsAsTable(conn, it) },
            sequence = sequence,
        )
    }

    private fun existsAsTable(conn: Connection, table: String): Boolean =
        conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL").use { ps ->
            ps.setString(1, "pgmq.${Identifiers.quote(table)}")
            ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
        }

    private fun tablePrivileges(conn: Connection, role: String, table: String): Set<String> {
        val candidates = listOf("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")
        return conn.prepareStatement("SELECT has_table_privilege(?, ?, ?)").use { ps ->
            candidates.filterTo(mutableSetOf()) { privilege ->
                ps.setString(1, role)
                ps.setString(2, "pgmq.${Identifiers.quote(table)}")
                ps.setString(3, privilege)
                ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
            }
        }
    }
}
