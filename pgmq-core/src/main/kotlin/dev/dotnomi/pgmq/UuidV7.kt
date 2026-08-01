package dev.dotnomi.pgmq

import java.security.SecureRandom
import java.util.UUID

/**
 * Generates time-ordered UUIDs (RFC 9562 version 7).
 *
 * Version 7 rather than 4 so ids sort chronologically and index well as primary keys.
 *
 * Layout: 48 bits unix_ts_ms, 4 bits version, 12 bits sequence, 2 bits variant, 62 bits random.
 */
object UuidV7 {
    private const val VERSION_BITS = 0x7L shl 12
    private const val MAX_SEQUENCE = 0xFFF
    private const val TIMESTAMP_MASK = 0xFFFF_FFFF_FFFFL
    private const val RANDOM_B_MASK = 0x3FFF_FFFF_FFFF_FFFFL
    private const val VARIANT_BITS = Long.MIN_VALUE

    private val random = SecureRandom()
    private val lock = Any()

    private var lastTimestamp = -1L
    private var sequence = 0

    fun generate(): UUID {
        val timestamp: Long
        val seq: Int

        synchronized(lock) {
            var now = System.currentTimeMillis()
            if (now < lastTimestamp) now = lastTimestamp

            if (now == lastTimestamp) {
                sequence++
                if (sequence > MAX_SEQUENCE) {
                    now = lastTimestamp + 1
                    sequence = 0
                }
            } else {
                sequence = 0
            }

            lastTimestamp = now
            timestamp = now
            seq = sequence
        }

        val mostSignificantBits = ((timestamp and TIMESTAMP_MASK) shl 16) or VERSION_BITS or seq.toLong()
        val leastSignificantBits = (random.nextLong() and RANDOM_B_MASK) or VARIANT_BITS
        return UUID(mostSignificantBits, leastSignificantBits)
    }

    fun generateString(): String = generate().toString()
}
