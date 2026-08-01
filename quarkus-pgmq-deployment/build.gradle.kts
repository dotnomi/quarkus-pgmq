plugins {
    java
    `java-library`
}

// Deliberately Java, not Kotlin. This module is build-time-only code that never reaches the user's
// runtime classpath, and `quarkus-extension-processor` is a Java annotation processor — running it
// over Kotlin sources via kapt is a known friction point with no upside here. Core and runtime stay
// Kotlin throughout.
description = "Quarkus extension for PGMQ — deployment module (build-time augmentation)"

dependencies {
    // platform, not enforcedPlatform: an enforced platform is published as a forced constraint and
    // would pin every consumer to exactly this Quarkus version. A plain platform supplies the
    // versions here while letting the consuming application's own BOM win.
    implementation(platform(libs.quarkus.bom))

    implementation(project(":quarkus-pgmq"))
    implementation(libs.quarkus.core.deployment)
    implementation(libs.quarkus.arc.deployment)
    implementation(libs.quarkus.agroal.deployment)

    annotationProcessor(platform(libs.quarkus.bom))
    annotationProcessor(libs.quarkus.extension.processor)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.quarkus.junit5.internal)
    testImplementation(libs.quarkus.jdbc.postgresql)
    testImplementation(libs.awaitility)
    // Metrics are optional for consumers, so they must be tested with the registry
    // actually present — that is the only path where the beans get registered.
    testImplementation(libs.quarkus.micrometer.registry.prometheus)
    testRuntimeOnly(libs.junit.platform.launcher)
}
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("-parameters") }
