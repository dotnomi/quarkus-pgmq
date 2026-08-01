package dev.dotnomi.pgmq.internal

/**
 * Queue names travel to pgmq as bind parameters and are only length-checked here; pgmq validates the
 * rest. Values that must be interpolated into SQL (GRANT, LISTEN channels) go through the strict
 * whitelist instead.
 */
internal object Identifiers {

    const val MAX_QUEUE_NAME_LENGTH = 47

    private val SAFE_IDENTIFIER = Regex("^[A-Za-z_][A-Za-z0-9_]{0,46}$")

    fun requireValidQueueName(queue: String): String {
        require(queue.isNotBlank()) { "Queue name must not be blank." }
        require(queue.length <= MAX_QUEUE_NAME_LENGTH) {
            "Queue name '$queue' is ${queue.length} characters long; pgmq allows at most " +
                "$MAX_QUEUE_NAME_LENGTH."
        }
        return queue
    }

    /** For values interpolated into SQL, where no bind parameter is possible. */
    fun requireSafeIdentifier(value: String, what: String): String {
        require(SAFE_IDENTIFIER.matches(value)) {
            "$what '$value' is not a valid SQL identifier. Allowed are letters, digits and " +
                "underscores, starting with a letter or underscore, at most 47 characters."
        }
        return value
    }

    fun quote(value: String): String = "\"${value.replace("\"", "\"\"")}\""
}
