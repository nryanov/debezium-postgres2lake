plugins {
    java
    id("io.quarkus")
}

group = "debezium-postgres2lake"
version = "0.1.0"

repositories {
    mavenCentral()
    mavenLocal()
    maven {
        url = uri("https://packages.confluent.io/maven")
    }
}

dependencies {
    implementation(enforcedPlatform(libs.quarkus.platform)) {
        // prefer debezium versions
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
//    implementation(platform(libs.debezium.platform)) {
//        // prefer quarkus versions
//        exclude(group = "org.slf4j", module = "slf4j-api")
//        exclude(group = "io.grpc")
//        exclude(group = "com.google.api.grpc")
//        exclude(group = "com.fasterxml.jackson.core")
//        exclude(group = "com.fasterxml.jackson.datatype")
//        exclude(group = "com.fasterxml.jackson.dataformat")
//        exclude(group = "org.postgresql")
//        exclude(group = "io.netty")
//        exclude(group = "org.junit.jupiter")
//    }
    implementation(enforcedPlatform(libs.aws.platform))
    implementation(enforcedPlatform(libs.iceberg.platform)) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    // iceberg
    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-api")
    implementation("org.apache.iceberg:iceberg-data")
    implementation("org.apache.iceberg:iceberg-aws")
    implementation("org.apache.iceberg:iceberg-parquet")
    implementation("org.apache.iceberg:iceberg-orc")

    // paimon
    // todo: exclude caffeine cache
    implementation(libs.paimon.core) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.paimon.common) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.paimon.s3) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.paimon.format) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    // s3
    implementation("software.amazon.awssdk:aws-core")
    implementation("software.amazon.awssdk:regions")
    implementation("software.amazon.awssdk:auth")
    implementation("software.amazon.awssdk:sdk-core")
    implementation("software.amazon.awssdk:http-auth")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk:netty-nio-client")
    // hadoop
    implementation(libs.hadoop.common) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.logback")
    }
    implementation(libs.hadoop.client)
    implementation(libs.hadoop.aws) {
        exclude(group = "software.amazon.awssdk", module = "bundle")
    }
    // hive
    implementation("org.apache.hive:hive-exec:4.2.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "asm")
        exclude(group = "org.apache.logging.log4j")
    }
    // quarkus
    implementation("io.quarkus.arc:arc")
    implementation("io.quarkus:quarkus-core")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
    // debezium
//    implementation("io.debezium:debezium-core")
//    implementation("io.debezium:debezium-api")
//    implementation("io.debezium:debezium-embedded")
//    implementation("io.debezium:debezium-connector-postgres")

    implementation(libs.debezium.embedded)
    implementation(libs.debezium.api)
    implementation(libs.debezium.storage.jdbc)
    implementation(libs.debezium.connector.postgresql)

    // parquet
    implementation(libs.parquet.avro) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    // orc
    implementation(libs.orc.core) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    // avro
    implementation(libs.confluent.avro)
    implementation(libs.confluent.kafka.avro.serializer)

    // test dependencies
    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(libs.awaitility)
    testImplementation("io.quarkus:quarkus-junit")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:minio")
    testImplementation("org.testcontainers:junit-jupiter")
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    jvmArgs(
        "--add-opens", "java.base/java.io=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.math=ALL-UNNAMED",
        "--add-opens", "java.base/java.net=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/java.text=ALL-UNNAMED",
        "--add-opens", "java.base/java.time=ALL-UNNAMED",
        "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens", "java.base/java.util.regex=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.reflect=ALL-UNNAMED",
        "--add-opens", "java.sql/java.sql=ALL-UNNAMED",
        "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
        "-Djava.security.manager=allow"
    )
}
