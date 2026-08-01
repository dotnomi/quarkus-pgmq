package dev.dotnomi.pgmq.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

/**
 * Carries a deserialization target type including generics, which `Class<T>` cannot express.
 *
 * Obtain one via [pgmqType] or [PgmqType.of].
 */
abstract class PgmqType<T> protected constructor() {

    /** Lazy because [of] overrides this instead of resolving the signature. */
    open val type: Type by lazy {
        (javaClass.genericSuperclass as? ParameterizedType)
            ?.actualTypeArguments?.firstOrNull()
            ?: error(
                "PgmqType must be created with a concrete type argument, for example " +
                    "'pgmqType<OrderDto>()' or 'object : PgmqType<OrderDto>() {}'.",
            )
    }

    override fun toString(): String = "PgmqType<$type>"

    companion object {
        fun <T : Any> of(clazz: Class<T>): PgmqType<T> = ClassPgmqType(clazz)
    }

    private class ClassPgmqType<T : Any>(clazz: Class<T>) : PgmqType<T>() {
        override val type: Type = clazz
    }
}

inline fun <reified T> pgmqType(): PgmqType<T> = object : PgmqType<T>() {}

/** Converts payloads between object and JSON. */
interface PgmqSerializer {
    fun serialize(value: Any?): String
    fun <T> deserialize(json: String, type: PgmqType<T>): T
}

/** Permanent failure: the payload does not match the expected type, so a retry cannot help. */
class PgmqDeserializationException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

/** Default serializer: Jackson with Kotlin and java.time support. */
class JacksonPgmqSerializer(
    val mapper: ObjectMapper = defaultMapper(),
) : PgmqSerializer {

    override fun serialize(value: Any?): String = mapper.writeValueAsString(value)

    override fun <T> deserialize(json: String, type: PgmqType<T>): T {
        val javaType = mapper.typeFactory.constructType(type.type)
        return try {
            @Suppress("UNCHECKED_CAST")
            mapper.readValue<Any?>(json, javaType) as T
        } catch (e: Exception) {
            throw PgmqDeserializationException(
                "Payload cannot be read as $javaType: ${e.message}", e,
            )
        }
    }

    companion object {
        fun defaultMapper(): ObjectMapper = ObjectMapper()
            .registerModule(KotlinModule.Builder().build())
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }
}
