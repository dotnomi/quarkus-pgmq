package dev.dotnomi.pgmq.support

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

/**
 * Absicherung gegen eine Falle, die bereits zugeschlagen hat: **JUnit Jupiter erkennt nur
 * Testmethoden mit `void`-Rueckgabe.**
 *
 * In Kotlin verleitet der Ausdrucksrumpf dazu, versehentlich einen Wert zurueckzugeben —
 * `fun test() = withQueue { … assertThat(x).hasSize(1) }` gibt den AssertJ-Assert zurueck und wird
 * damit **stillschweigend nicht ausgefuehrt**. Kein Fehler, keine Warnung, der Test zaehlt einfach
 * nicht mit. Beim ersten Durchlauf dieses Projekts verschwanden so 18 von 27 Tests.
 *
 * Dieser Test laedt die kompilierten Testklassen und schlaegt Alarm, wenn eine `@Test`-Methode
 * etwas anderes als `void` zurueckgibt.
 */
class NoSilentlySkippedTestsTest {
    @Test
    fun `every @Test method returns Unit so JUnit actually discovers it`() {
        val classesRoot = File(
            javaClass.protectionDomain.codeSource.location.toURI(),
        )
        assertThat(classesRoot)
            .describedAs("Wurzel der kompilierten Testklassen")
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
                            offenders += "${clazz.name}#${method.name} gibt ${method.returnType.simpleName} zurueck"
                        }
                    }
                }
        }

        assertThat(offenders)
            .describedAs(
                "Diese @Test-Methoden werden von JUnit NICHT ausgefuehrt, weil sie einen Wert " +
                    "zurueckgeben. In Kotlin den Rueckgabetyp explizit auf ': Unit' setzen oder " +
                    "einen Block-Rumpf verwenden.",
            )
            .isEmpty()
    }
}
