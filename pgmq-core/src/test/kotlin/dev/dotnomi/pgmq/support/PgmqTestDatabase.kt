package dev.dotnomi.pgmq.support

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dev.dotnomi.pgmq.PgmqTemplate
import java.util.UUID
import javax.sql.DataSource

/**
 * Access to the locally running pgmq container. Connection details come from system properties set
 * by the root `build.gradle.kts`.
 */
object PgmqTestDatabase {
    val dataSource: DataSource by lazy {
        HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres")
                username = System.getProperty("pgmq.test.user", "postgres")
                password = System.getProperty("pgmq.test.password", "postgres")
                // Plenty of connections, so the concurrency tests block on the behaviour under
                // test rather than on the pool.
                maximumPoolSize = 16
                poolName = "pgmq-test"
            },
        )
    }

    fun template(sourceId: String = "test-source"): PgmqTemplate =
        PgmqTemplate(dataSource, sourceId = sourceId)

    /**
     * Creates a randomly named queue, runs [block] and cleans up afterwards — including when the
     * test fails. The random name keeps parallel test runs out of each other's way.
     */
    fun <R> withQueue(prefix: String = "t", fifo: Boolean = false, block: (String) -> R): R {
        val template = template()
        val queue = uniqueQueueName(prefix)
        template.createQueue(queue)
        if (fifo) template.createFifoIndex(queue)
        return try {
            block(queue)
        } finally {
            runCatching { template.dropQueue(queue) }
        }
    }

    fun uniqueQueueName(prefix: String = "t"): String =
        "${prefix}_${UUID.randomUUID().toString().replace("-", "").take(16)}"
}

/** Test payload with several field types, so serialization is genuinely exercised. */
data class OrderDto(
    val orderId: String,
    val amountCents: Long,
    val items: List<String>,
    val note: String? = null,
)
