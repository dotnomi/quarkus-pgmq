package dev.dotnomi.pgmq.it

import dev.dotnomi.pgmq.PgmqExchangeContext
import dev.dotnomi.pgmq.quarkus.PgmqHeader
import dev.dotnomi.pgmq.quarkus.PgmqListener
import dev.dotnomi.pgmq.quarkus.PgmqPublisher
import dev.dotnomi.pgmq.quarkus.PgmqTopicListener
import dev.dotnomi.pgmq.topics.SubscriptionMode
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A small but realistic flow, used to exercise the extension the way an application would.
 *
 * An order is published, a listener reacts to it and publishes a mail, and a topic listener sees a
 * broadcast. The point is the chain: it proves that publishing from *inside* a handler propagates
 * the correlation identifiers automatically, which is the behaviour that makes those fields worth
 * having.
 */
data class Order(val orderId: String, val amountCents: Long)

data class Mail(val recipient: String, val subject: String)

data class Announcement(val text: String)

object Received {
    val orders: MutableList<String> = CopyOnWriteArrayList()
    val mails: MutableList<String> = CopyOnWriteArrayList()
    val announcements: MutableList<String> = CopyOnWriteArrayList()

    /** Correlation ids seen by each stage, used to assert the chain end to end. */
    val orderCorrelations: MutableList<String> = CopyOnWriteArrayList()
    val mailCorrelations: MutableList<String> = CopyOnWriteArrayList()
    val mailCausations: MutableList<String> = CopyOnWriteArrayList()

    fun clear() {
        listOf(orders, mails, announcements, orderCorrelations, mailCorrelations, mailCausations)
            .forEach { it.clear() }
    }
}

@ApplicationScoped
@PgmqPublisher(queue = OrderFlow.ORDERS, targetId = "fulfilment")
class OrderPublisher {
    fun publishOrder(order: Order) {
        throw AssertionError("replaced by the interceptor")
    }
}

@ApplicationScoped
@PgmqPublisher(queue = OrderFlow.MAILS, targetId = "mailer")
class MailPublisher {
    fun sendMail(mail: Mail, @PgmqHeader("priority") priority: String) {
        throw AssertionError("replaced by the interceptor")
    }
}

@ApplicationScoped
class OrderFlow {

    @Inject
    lateinit var mails: MailPublisher

    @PgmqListener(queue = ORDERS, pollInterval = "200ms")
    fun onOrder(order: Order) {
        Received.orders += order.orderId
        PgmqExchangeContext.current()?.let { Received.orderCorrelations += it.correlationId }

        // Published from inside a handler: correlationId is inherited and causationId is set to this
        // message's id, without the handler doing anything about it.
        mails.sendMail(Mail("customer@example.com", "Order ${order.orderId}"), "high")
    }

    @PgmqListener(queue = MAILS, pollInterval = "200ms")
    fun onMail(mail: Mail) {
        Received.mails += mail.subject
        PgmqExchangeContext.current()?.let {
            Received.mailCorrelations += it.correlationId
            it.causationId?.let { causation -> Received.mailCausations += causation }
        }
    }

    @PgmqTopicListener(topic = ANNOUNCEMENTS, mode = SubscriptionMode.BROADCAST, pollInterval = "200ms")
    fun onAnnouncement(announcement: Announcement) {
        Received.announcements += announcement.text
    }

    companion object {
        const val ORDERS = "it_orders"
        const val MAILS = "it_mails"
        const val ANNOUNCEMENTS = "it_announcements"
    }
}
