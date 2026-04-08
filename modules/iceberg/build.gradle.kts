plugins {
    java
    id("io.quarkus")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation(project(":modules:jib"))
    implementation(project(":modules:domain"))
    implementation(project(":modules:core"))

    implementation(enforcedPlatform(libs.iceberg.platform)) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-api")
    implementation("org.apache.iceberg:iceberg-data")
    implementation("org.apache.iceberg:iceberg-aws")
    implementation("org.apache.iceberg:iceberg-parquet")
    implementation("org.apache.iceberg:iceberg-orc")
    implementation("org.apache.iceberg:iceberg-nessie")
    implementation("org.apache.iceberg:iceberg-hive-metastore")

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
    testImplementation(testFixtures(project(":modules:test-fixtures:s3")))
    testImplementation(testFixtures(project(":modules:test-fixtures:postgres")))
    testImplementation(testFixtures(project(":modules:test-fixtures:schema-registry")))
    testImplementation(testFixtures(project(":modules:test-fixtures:nessie")))
}
