package dev.dotnomi.pgmq.quarkus

import jakarta.annotation.Priority
import jakarta.interceptor.AroundInvoke
import jakarta.interceptor.Interceptor
import jakarta.interceptor.InvocationContext

/**
 * Applies [PgmqPublisher] to a concrete class: the call publishes and the method body never runs.
 *
 * Never calls `proceed()`, which is what makes the body dead. Publishers declared as interfaces go
 * through a generated proxy instead — see [PgmqPublishInvoker].
 */
@Interceptor
@PgmqPublisher
@Priority(Interceptor.Priority.APPLICATION)
class PgmqPublisherInterceptor(private val invoker: PgmqPublishInvoker) {

    @AroundInvoke
    fun publish(context: InvocationContext): Any? =
        invoker.invoke(context.method, context.parameters ?: emptyArray())
}
