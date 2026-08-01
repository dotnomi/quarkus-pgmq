plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.quarkus.extension)
    `java-library`
}

description = "Quarkus extension for PGMQ — runtime module"

quarkusExtension {
    deploymentModule = "quarkus-pgmq-deployment"
}

dependencies {
    // platform, not enforcedPlatform: an enforced platform is published as a forced constraint and
    // would pin every consumer to exactly this Quarkus version. A plain platform supplies the
    // versions here while letting the consuming application's own BOM win.
    implementation(platform(libs.quarkus.bom))

    api(project(":pgmq-core"))
    api(libs.quarkus.core)
    implementation(libs.quarkus.arc)

    // pgmq-core logs through SLF4J, but a Quarkus application logs through JBoss LogManager. Without
    // this bridge every warning the library emits — dead-lettered messages, failed handlers, stale
    // topic subscriptions — is silently discarded. The native build made that visible: "No SLF4J
    // providers were found".
    runtimeOnly(libs.slf4j.jboss.logmanager)
    implementation(libs.quarkus.agroal)

    // Optional: metrics classes are only loaded when the application brings Micrometer along,
    // and the deployment module registers them only then. compileOnly keeps it off the
    // dependency graph of everyone who does not want metrics.
    compileOnly(libs.quarkus.micrometer)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}
