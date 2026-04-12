plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation(project(":modules:domain"))
    api(project(":extensions:data-catalog-api"))
    api(project(":extensions:commit-event-emitter-api"))

    api("software.amazon.awssdk:aws-core")
    api("software.amazon.awssdk:regions")
    api("software.amazon.awssdk:auth")
    api("software.amazon.awssdk:sdk-core")
    api("software.amazon.awssdk:http-auth")
    api("software.amazon.awssdk:s3-transfer-manager")
    api("software.amazon.awssdk:netty-nio-client")

    api(libs.hadoop.common) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.logback")
    }
    api(libs.hadoop.client)
    api(libs.hadoop.aws) {
        exclude(group = "software.amazon.awssdk", module = "bundle")
    }

    api("io.quarkus.arc:arc")
    api("io.quarkus:quarkus-core")
    api("io.quarkus:quarkus-config-yaml")
    api("io.quarkus:quarkus-logging-json")
    api("io.quarkus:quarkus-micrometer-registry-prometheus")

    api("io.debezium:debezium-core")
    api("io.debezium:debezium-api")
    api("io.debezium:debezium-embedded")
    api("io.debezium:debezium-connector-postgres")

    api(libs.confluent.avro)
    api(libs.confluent.avro.serializer)

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
}
