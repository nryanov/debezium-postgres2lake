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

rootProject.name = "debezium-server-lake"

include(
    ":debezium-server-lake-dist",
    ":debezium-server-parquet-sink",
    ":debezium-server-orc-sink",
    ":debezium-server-iceberg-sink",
    ":debezium-server-paimon-sink"
)
