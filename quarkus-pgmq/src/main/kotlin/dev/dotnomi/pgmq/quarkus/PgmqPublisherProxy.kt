package dev.dotnomi.pgmq.quarkus

import io.quarkus.arc.Arc
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy

/**
 * Builds the implementation for a [PgmqPublisher] declared as an interface.
 *
 * An interface cannot be a CDI bean, so the interceptor route does not apply. A JDK dynamic proxy
 * takes its place: abstract methods publish, `default` methods run their own body, and the three
 * `Object` methods behave sensibly.
 */
object PgmqPublisherProxy {

    fun <T> create(publisherInterface: Class<T>): T {
        val handler = InvocationHandler { proxy, method, args ->
            val arguments = args ?: emptyArray()
            when {
                method.isDefault -> InvocationHandler.invokeDefault(proxy, method, *arguments)
                method.declaringClass == Any::class.java -> objectMethod(proxy, method, arguments)
                else -> invoker().invoke(method, arguments)
            }
        }

        @Suppress("UNCHECKED_CAST")
        return Proxy.newProxyInstance(
            publisherInterface.classLoader,
            arrayOf(publisherInterface),
            handler,
        ) as T
    }

    private fun objectMethod(proxy: Any, method: Method, args: Array<Any?>): Any? = when (method.name) {
        "equals" -> proxy === args.getOrNull(0)
        "hashCode" -> System.identityHashCode(proxy)
        else -> "PgmqPublisher proxy for ${proxy.javaClass.interfaces.firstOrNull()?.name}"
    }

    private fun invoker(): PgmqPublishInvoker =
        Arc.container().instance(PgmqPublishInvoker::class.java).get()
}
