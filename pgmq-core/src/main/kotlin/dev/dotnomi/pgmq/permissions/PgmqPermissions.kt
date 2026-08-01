package dev.dotnomi.pgmq.permissions

/**
 * What a role may do with a queue.
 *
 * Works because every pgmq function is SECURITY INVOKER, so plain table grants decide what a caller
 * can do.
 */
enum class PgmqPrivilege {
    /** `send` and `send_batch`. Also grants SELECT, which `INSERT … RETURNING` requires. */
    ENQUEUE,

    /** `read`, `pop`, `delete`, `set_vt`. Includes UPDATE, which `pgmq.read` needs. */
    DEQUEUE,

    /** Moving messages into the archive table. Needs [DEQUEUE] to be useful. */
    ARCHIVE,

    /** `purge_queue`. */
    PURGE,

    /** Read-only: metrics, queue contents, and listing queues. */
    MONITOR,

    /** Everything. */
    ADMIN,
}

/** The privileges a role holds on a queue, read back from the catalog. */
data class EffectivePermissions(
    val queue: String,
    val role: String,
    val privileges: Set<PgmqPrivilege>,
    /** Raw table-level grants, for diagnosing unexpected behaviour. */
    val rawGrants: Map<String, Set<String>>,
) {
    fun canEnqueue(): Boolean = PgmqPrivilege.ENQUEUE in privileges || PgmqPrivilege.ADMIN in privileges
    fun canDequeue(): Boolean = PgmqPrivilege.DEQUEUE in privileges || PgmqPrivilege.ADMIN in privileges
}
