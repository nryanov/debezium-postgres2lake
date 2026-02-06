plugins {
    id("io.quarkus")
    id("java")
}

dependencies {
    implementation(project(":debezium-server-parquet-sink"))
    implementation(project(":debezium-server-orc-sink"))
    implementation(project(":debezium-server-iceberg-sink"))
    implementation(project(":debezium-server-paimon-sink"))

    implementation(libs.debezium.server.dist)
}
