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
    implementation(enforcedPlatform(libs.debezium.platform)) {
        // prefer quarkus versions
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "io.grpc")
        exclude(group = "com.google.api.grpc")
        exclude(group = "com.fasterxml.jackson.core")
        exclude(group = "com.fasterxml.jackson.datatype")
        exclude(group = "com.fasterxml.jackson.dataformat")
        exclude(group = "org.postgresql")
        exclude(group = "io.netty")
    }
    implementation(enforcedPlatform(libs.aws.platform))
    implementation(enforcedPlatform(libs.iceberg.platform)) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    // iceberg
    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-api")
    implementation("org.apache.iceberg:iceberg-data")
    implementation("org.apache.iceberg:iceberg-parquet")
    implementation("org.apache.iceberg:iceberg-orc")

    // s3
    implementation("software.amazon.awssdk:aws-core")
    implementation("software.amazon.awssdk:regions")
    implementation("software.amazon.awssdk:auth")
    implementation("software.amazon.awssdk:sdk-core")
    implementation("software.amazon.awssdk:http-auth")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk:netty-nio-client")
    // hadoop
    implementation(libs.hadoop.common)
    implementation(libs.hadoop.client)
    implementation(libs.hadoop.aws) {
        exclude(group = "software.amazon.awssdk", module = "bundle")
    }
    // hive
    implementation("org.apache.hive:hive-exec:4.2.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "asm")
    }
    // quarkus
    implementation("io.quarkus.arc:arc")
    implementation("io.quarkus:quarkus-core")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
    // debezium
    implementation("io.debezium:debezium-core")
    implementation("io.debezium:debezium-api")
    implementation("io.debezium:debezium-embedded")
    implementation("io.debezium:debezium-connector-postgres")
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

    // Test dependencies
    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.testcontainers.bom))
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
