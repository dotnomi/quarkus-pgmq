package dev.dotnomi.pgmq.permissions

import dev.dotnomi.pgmq.PgmqTemplate
import dev.dotnomi.pgmq.read
import dev.dotnomi.pgmq.support.OrderDto
import dev.dotnomi.pgmq.support.PgmqTestDatabase
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.util.UUID
import javax.sql.DataSource

/**
 * Proves the separation actually holds at the database level: a publish-only role really cannot
 * consume, and a consume-only role really cannot publish.
 *
 * Every assertion runs through a **real connection as that role**, not through the admin connection
 * — a permission test that only inspects the catalog would pass even if the rights were never
 * enforced.
 */
class PgmqPermissionsTest {
    private val admin = PgmqTestDatabase.template()

    /** Creates a login role, runs [block] with a template connected as that role, then drops it. */
    private fun withRole(prefix: String, block: (role: String, template: PgmqTemplate) -> Unit) {
        val role = "${prefix}_${UUID.randomUUID().toString().replace("-", "").take(10)}"
        val password = "test-password"

        PgmqTestDatabase.dataSource.connection.use { conn ->
            conn.createStatement().use {
                it.execute("CREATE ROLE \"$role\" LOGIN PASSWORD '$password'")
            }
        }

        try {
            block(role, PgmqTemplate(roleDataSource(role, password), sourceId = role))
        } finally {
            PgmqTestDatabase.dataSource.connection.use { conn ->
                conn.createStatement().use {
                    runCatching { it.execute("REASSIGN OWNED BY \"$role\" TO CURRENT_USER") }
                    runCatching { it.execute("DROP OWNED BY \"$role\"") }
                    runCatching { it.execute("DROP ROLE \"$role\"") }
                }
            }
        }
    }

    /** Minimal DataSource that opens a fresh connection as the given role. */
    private fun roleDataSource(role: String, password: String): DataSource {
        val url = System.getProperty("pgmq.test.jdbc-url", "jdbc:postgresql://localhost:5432/postgres")
        return object : DataSource by PgmqTestDatabase.dataSource {
            override fun getConnection() = DriverManager.getConnection(url, role, password)
            override fun getConnection(username: String?, pw: String?) = getConnection()
        }
    }

    @Test
    fun `enqueue-only role can publish but not consume`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("pub") { role, asRole ->
                admin.permissions().grant(queue, role, PgmqPrivilege.ENQUEUE)

                val msgId = asRole.send(queue, OrderDto("P-1", 1, emptyList()))
                assertThat(msgId).isPositive()

                assertThatThrownBy { asRole.read<OrderDto>(queue) }
                    .describedAs("ENQUEUE must not imply the ability to read")
                    .hasMessageContaining("permission denied")

                // The admin connection still sees the message, so the publish really happened.
                assertThat(admin.metrics(queue).length).isEqualTo(1)
            }
        }
    }

    @Test
    fun `dequeue-only role can consume but not publish`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("sub") { role, asRole ->
                admin.permissions().grant(queue, role, PgmqPrivilege.DEQUEUE)
                admin.send(queue, OrderDto("P-2", 1, emptyList()))

                val messages = asRole.read<OrderDto>(queue)
                assertThat(messages).hasSize(1)
                assertThat(asRole.delete(queue, messages.single().msgId)).isTrue()

                assertThatThrownBy { asRole.send(queue, OrderDto("P-3", 1, emptyList())) }
                    .describedAs("DEQUEUE must not imply the ability to publish")
                    .hasMessageContaining("permission denied")
            }
        }
    }

    @Test
    fun `read needs UPDATE because it writes the visibility timeout back`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("ro") { role, asRole ->
                // Deliberately only SELECT — the mistake someone makes when assuming reading is
                // read-only. pgmq.read updates vt, read_ct and last_read_at, so it fails.
                admin.permissions().grant(queue, role, PgmqPrivilege.MONITOR)
                admin.send(queue, OrderDto("P-4", 1, emptyList()))

                assertThatThrownBy { asRole.read<OrderDto>(queue) }
                    .describedAs("pgmq.read performs an UPDATE, so SELECT alone is not enough")
                    .hasMessageContaining("permission denied")
            }
        }
    }

    @Test
    fun `archive privilege allows moving messages into the archive table`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("arch") { role, asRole ->
                admin.permissions().grant(queue, role, PgmqPrivilege.DEQUEUE, PgmqPrivilege.ARCHIVE)
                admin.send(queue, OrderDto("P-5", 1, emptyList()))

                val message = asRole.read<OrderDto>(queue).single()
                assertThat(asRole.archive(queue, message.msgId)).isTrue()
                assertThat(admin.metrics(queue).length).isZero()
            }
        }
    }

    @Test
    fun `revoke takes the right away again`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("rev") { role, asRole ->
                admin.permissions().grant(queue, role, PgmqPrivilege.ENQUEUE)
                assertThat(asRole.send(queue, OrderDto("P-6", 1, emptyList()))).isPositive()

                admin.permissions().revoke(queue, role, PgmqPrivilege.ENQUEUE)

                assertThatThrownBy { asRole.send(queue, OrderDto("P-7", 1, emptyList())) }
                    .hasMessageContaining("permission denied")
            }
        }
    }

    @Test
    fun `effective reports what the role can really do`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("eff") { role, _ ->
                admin.permissions().grant(queue, role, PgmqPrivilege.ENQUEUE)

                val afterEnqueue = admin.permissions().effective(queue, role)
                assertThat(afterEnqueue.canEnqueue()).isTrue()
                assertThat(afterEnqueue.canDequeue()).isFalse()
                assertThat(afterEnqueue.privileges).contains(PgmqPrivilege.ENQUEUE)

                admin.permissions().grant(queue, role, PgmqPrivilege.DEQUEUE)

                val afterDequeue = admin.permissions().effective(queue, role)
                assertThat(afterDequeue.canDequeue()).isTrue()
                assertThat(afterDequeue.privileges)
                    .contains(PgmqPrivilege.ENQUEUE, PgmqPrivilege.DEQUEUE, PgmqPrivilege.MONITOR)
            }
        }
    }

    @Test
    fun `reset removes every right at once`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("res") { role, _ ->
                admin.permissions().grant(queue, role, PgmqPrivilege.ADMIN)
                assertThat(admin.permissions().effective(queue, role).privileges)
                    .contains(PgmqPrivilege.ADMIN)

                admin.permissions().reset(queue, role)

                val remaining = admin.permissions().effective(queue, role)
                assertThat(remaining.privileges).isEmpty()
            }
        }
    }

    @Test
    fun `a publishing role cannot enumerate other queues`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("leak") { role, asRole ->
                admin.permissions().grant(queue, role, PgmqPrivilege.ENQUEUE)

                // Its own queue works.
                assertThat(asRole.send(queue, OrderDto("L-1", 1, emptyList()))).isPositive()

                // But pgmq.meta stays out of reach, so the role cannot see what else exists.
                assertThatThrownBy { asRole.listQueues() }
                    .describedAs(
                        "granting SELECT on pgmq.meta would expose every queue name in the database " +
                            "to every publisher — and it is not needed for send/read/archive",
                    )
                    .hasMessageContaining("permission denied")
            }
        }
    }

    @Test
    fun `monitor can read metrics which needs the sequence too`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            withRole("mon") { role, asRole ->
                admin.permissions().grant(queue, role, PgmqPrivilege.MONITOR)
                admin.send(queue, OrderDto("MO-1", 1, emptyList()))

                // metrics() reads the sequence's last_value; SELECT on the table alone would fail.
                assertThat(asRole.metrics(queue).length).isEqualTo(1)
            }
        }
    }

    @Test
    fun `role names are validated because GRANT cannot use bind parameters`() {
        PgmqTestDatabase.withQueue("perm") { queue ->
            assertThatThrownBy {
                admin.permissions().grant(queue, "evil\"; DROP TABLE pgmq.meta; --", PgmqPrivilege.ENQUEUE)
            }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("not a valid SQL identifier")
        }
    }

    @Test
    fun `granting on a missing queue fails with a clear message`() {
        withRole("missing") { role, _ ->
            assertThatThrownBy {
                admin.permissions().grant("does_not_exist_queue", role, PgmqPrivilege.ENQUEUE)
            }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("does not exist")
        }
    }
}
