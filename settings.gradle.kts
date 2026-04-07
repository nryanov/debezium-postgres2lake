pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
    plugins {
        id("io.quarkus") version "3.31.2"
        id("org.kordamp.gradle.jandex") version "2.3.0"
    }
}

rootProject.name = "debezium-postgres2lake"

include(
    "modules:platform",
    "modules:domain",
    "modules:core",
    "modules:avro",
    "modules:orc",
    "modules:parquet",
    "modules:iceberg",
    "modules:paimon",
    // fixtures
    "modules:test-fixtures:common",
    "modules:test-fixtures:s3",
    "modules:test-fixtures:postgres",
    "modules:test-fixtures:schema-registry",
    "modules:test-fixtures:nessie",
    "modules:test-fixtures:hive-metastore",
)

project(":modules").projectDir = file("modules")
project(":modules:test-fixtures").projectDir = file("modules/test-fixtures")

project(":modules:platform").projectDir = file("modules/platform")
project(":modules:domain").projectDir = file("modules/domain")
project(":modules:core").projectDir = file("modules/core")
project(":modules:avro").projectDir = file("modules/avro")
project(":modules:orc").projectDir = file("modules/orc")
project(":modules:parquet").projectDir = file("modules/parquet")
project(":modules:iceberg").projectDir = file("modules/iceberg")
project(":modules:paimon").projectDir = file("modules/paimon")

project(":modules:test-fixtures:common").projectDir = file("modules/test-fixtures/common")
project(":modules:test-fixtures:s3").projectDir = file("modules/test-fixtures/s3")
project(":modules:test-fixtures:postgres").projectDir = file("modules/test-fixtures/postgres")
project(":modules:test-fixtures:schema-registry").projectDir = file("modules/test-fixtures/schema-registry")
project(":modules:test-fixtures:nessie").projectDir = file("modules/test-fixtures/nessie")
project(":modules:test-fixtures:hive-metastore").projectDir = file("modules/test-fixtures/hive-metastore")
