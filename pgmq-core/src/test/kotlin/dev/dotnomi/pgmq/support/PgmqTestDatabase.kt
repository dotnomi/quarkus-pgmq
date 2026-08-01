package dev.dotnomi.pgmq.support

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dev.dotnomi.pgmq.PgmqTemplate
import java.util.UUID
import javax.sql.DataSource

/**
 * Zugang zum lokal laufenden pgmq-Container. Verbindungsdaten kommen aus System-Properties, die
 * das Root-`build.gradle.kts` setzt.
 */
object PgmqTestDatabase {
    val dataSource: DataSource by lazy {
        HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres")
                username = System.getProperty("pgmq.test.user", "postgres")
                password = System.getProperty("pgmq.test.password", "postgres")
                // Reichlich Connections, damit die Nebenlaeufigkeitstests nicht am Pool haengen
                // statt am zu untersuchenden Verhalten.
                maximumPoolSize = 16
                poolName = "pgmq-test"
            },
        )
    }

    fun template(sourceId: String = "test-source"): PgmqTemplate =
        PgmqTemplate(dataSource, sourceId = sourceId)

    /**
     * Erzeugt eine Queue mit zufaelligem Namen, fuehrt [block] aus und raeumt danach auf — auch wenn
     * der Test fehlschlaegt. Zufaellige Namen, damit parallele Testlaeufe sich nicht ins Gehege
     * kommen.
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

/** Testpayload mit mehreren Feldtypen, um die Serialisierung wirklich zu pruefen. */
data class OrderDto(
    val orderId: String,
    val amountCents: Long,
    val items: List<String>,
    val note: String? = null,
)
