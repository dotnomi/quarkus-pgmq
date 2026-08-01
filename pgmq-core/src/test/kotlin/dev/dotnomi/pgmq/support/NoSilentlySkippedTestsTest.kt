package dev.dotnomi.pgmq.support

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

/**
 * Guards against a trap that has already been sprung here: **JUnit Jupiter only recognises test
 * methods that return `void`.**
 *
 * Kotlin's expression body makes it easy to return a value by accident —
 * `fun test() = withQueue { … assertThat(x).hasSize(1) }` returns the AssertJ assertion and is
 * therefore **silently never run**. No error, no warning, the test simply does not count. On this
 * project's first run 18 of 27 tests vanished that way.
 *
 * This test loads the compiled test classes and raises the alarm when an `@Test` method returns
 * anything other than `void`.
 */
class NoSilentlySkippedTestsTest {
    @Test
    fun `every @Test method returns Unit so JUnit actually discovers it`() {
        val classesRoot = File(
            javaClass.protectionDomain.codeSource.location.toURI(),
        )
        assertThat(classesRoot)
            .describedAs("root of the compiled test classes")
            .exists()

        val offenders = mutableListOf<String>()

        Files.walk(classesRoot.toPath()).use { paths ->
            paths.filter { it.toString().endsWith(".class") }
                .forEach { path ->
                    val className = classesRoot.toPath().relativize(path)
                        .toString()
                        .removeSuffix(".class")
                        .replace(File.separatorChar, '.')

                    val clazz = runCatching { Class.forName(className, false, javaClass.classLoader) }
                        .getOrNull() ?: return@forEach

                    clazz.declaredMethods.forEach { method ->
                        val isTest = method.annotations.any {
                            it.annotationClass.qualifiedName == "org.junit.jupiter.api.Test"
                        }
                        if (isTest && method.returnType != Void.TYPE) {
                            offenders += "${clazz.name}#${method.name} returns ${method.returnType.simpleName}"
                        }
                    }
                }
        }

        assertThat(offenders)
            .describedAs(
                "JUnit does NOT run these @Test methods because they return a value. In Kotlin, " +
                    "state the return type as ': Unit' explicitly, or use a block body.",
            )
            .isEmpty()
    }
}
