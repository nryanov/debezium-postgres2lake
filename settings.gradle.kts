pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
    plugins {
        id("io.quarkus") version "3.31.2"
    }
}

rootProject.name = "debezium-postgres2lake"
