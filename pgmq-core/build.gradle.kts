plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
}

description = "Framework-neutral Kotlin library for PGMQ (Postgres Message Queue) on plain JDBC"

dependencies {
    api(libs.slf4j.api)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.jackson.datatype.jsr310)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.awaitility)
    testImplementation(libs.postgresql)
    testImplementation(libs.hikaricp)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}
