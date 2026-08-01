plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.quarkus)
}

// Consumes the extension the way an application would — through its published artifacts rather than
// the deployment module's own classpath. That is what makes this more than a duplicate of the
// QuarkusUnitTest suite: it exercises the extension descriptor and the runtime/deployment split.
description = "End-to-end application using the packaged quarkus-pgmq extension"

dependencies {
    implementation(enforcedPlatform(libs.quarkus.bom))
    implementation(project(":quarkus-pgmq"))
    implementation(libs.quarkus.kotlin)
    implementation(libs.quarkus.jdbc.postgresql)

    testImplementation(libs.quarkus.junit5)
    testImplementation(libs.assertj)
    testImplementation(libs.awaitility)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Native verification is configured but NOT exercised here: no GraalVM is installed on this machine.
// It matters for this extension in particular, because handler invocation goes through reflection
// and the ReflectiveClassBuildItem registration has never been proven against a native image.
//
//   ./gradlew :integration-tests:build -Dquarkus.native.enabled=true \
//       -Dquarkus.native.container-build=true
