plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "quarkus-pgmq-parent"

include(
    "pgmq-core",
    "quarkus-pgmq",
    "quarkus-pgmq-deployment",
    "integration-tests",
)

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
