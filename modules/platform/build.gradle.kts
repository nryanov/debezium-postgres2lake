plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    api(enforcedPlatform(libs.quarkus.platform)) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    api(enforcedPlatform(libs.debezium.platform)) {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "io.grpc")
        exclude(group = "com.google.api.grpc")
        exclude(group = "com.fasterxml.jackson.core")
        exclude(group = "com.fasterxml.jackson.datatype")
        exclude(group = "com.fasterxml.jackson.dataformat")
        exclude(group = "org.postgresql")
        exclude(group = "io.netty")
        exclude(group = "org.junit.jupiter")
    }
    api(enforcedPlatform(libs.aws.platform))
}
