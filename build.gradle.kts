plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    // Both Quarkus plugins are declared here, even though neither is applied at the root. Declaring
    // them in a single place puts them on one classloader; applying io.quarkus in one subproject and
    // io.quarkus.extension in another otherwise fails with a cross-classloader access error in
    // Quarkus' devtools classes.
    alias(libs.plugins.quarkus) apply false
    alias(libs.plugins.quarkus.extension) apply false
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.dokka) apply false
}

allprojects {
    // The Maven Central namespace, which is verified per group. The Kotlin packages stay
    // dev.dotnomi.pgmq — a package name has no relationship to the publishing coordinates.
    group = "io.github.dotnomi"
    version = providers.gradleProperty("pgmqVersion").getOrElse("0.1.0-SNAPSHOT")
}

subprojects {
    // Every module: JDK 21 toolchain, bytecode target 17 (the Quarkus baseline, so the artifacts
    // stay maximally compatible; they run fine under Java 25).
    plugins.withId("java") {
        extensions.configure<JavaPluginExtension> {
            toolchain.languageVersion.set(JavaLanguageVersion.of(21))
        }
        tasks.withType<JavaCompile>().configureEach {
            options.release.set(17)
        }
        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            // Connection to the local pgmq container; override with -Ppgmq.test.* or the environment.
            systemProperty(
                "pgmq.test.jdbc-url",
                providers.gradleProperty("pgmq.test.jdbc-url")
                    .orElse(providers.environmentVariable("PGMQ_TEST_JDBC_URL"))
                    .getOrElse("jdbc:postgresql://localhost:5432/postgres"),
            )
            systemProperty(
                "pgmq.test.user",
                providers.gradleProperty("pgmq.test.user")
                    .orElse(providers.environmentVariable("PGMQ_TEST_USER"))
                    .getOrElse("postgres"),
            )
            systemProperty(
                "pgmq.test.password",
                providers.gradleProperty("pgmq.test.password")
                    .orElse(providers.environmentVariable("PGMQ_TEST_PASSWORD"))
                    .getOrElse("postgres"),
            )
            testLogging {
                events("failed", "skipped")
                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            }
        }
    }

    plugins.withId("org.jetbrains.kotlin.jvm") {
        extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
            compilerOptions {
                jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
                freeCompilerArgs.addAll("-Xjsr305=strict", "-java-parameters")
            }
        }
    }
}

// Publication to Maven Central. Credentials and the signing key come from environment variables
// (ORG_GRADLE_PROJECT_*), so nothing secret ever lives in the repository.
//
// All three modules are published, including quarkus-pgmq-deployment: the runtime jar names it as
// its deployment-artifact, and Quarkus resolves it during every consumer's build. Leaving it out
// would break every application that depends on the extension.
configure(subprojects.filter { it.name != "integration-tests" }) {
    apply(plugin = "com.vanniktech.maven.publish")
    if (name != "quarkus-pgmq-deployment") {
        apply(plugin = "org.jetbrains.dokka")
    }

    extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral(automaticRelease = true)
        // Central rejects unsigned artifacts, but signing unconditionally would break every local
        // publishToMavenLocal with "no configured signatory". Gradle maps the environment variable
        // ORG_GRADLE_PROJECT_signingInMemoryKey onto this property, so CI signs and a developer
        // machine without a key does not.
        if (providers.gradleProperty("signingInMemoryKey").isPresent) {
            signAllPublications()
        }

        pom {
            name.set(project.name)
            description.set(project.description)
            url.set("https://github.com/dotnomi/quarkus-pgmq")
            inceptionYear.set("2026")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    distribution.set("repo")
                }
            }
            developers {
                developer {
                    id.set("dotnomi")
                    name.set("dotnomi")
                    url.set("https://github.com/dotnomi")
                }
            }
            scm {
                url.set("https://github.com/dotnomi/quarkus-pgmq")
                connection.set("scm:git:git://github.com/dotnomi/quarkus-pgmq.git")
                developerConnection.set("scm:git:ssh://git@github.com/dotnomi/quarkus-pgmq.git")
            }
        }
    }
}
