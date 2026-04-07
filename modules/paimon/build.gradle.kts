plugins {
    java
    id("io.quarkus")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation(project(":modules:domain"))
    implementation(project(":modules:core"))

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
    implementation(libs.paimon.hive.catalog) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
    testImplementation(testFixtures(project(":modules:test-fixtures:s3")))
    testImplementation(testFixtures(project(":modules:test-fixtures:postgres")))
    testImplementation(testFixtures(project(":modules:test-fixtures:schema-registry")))
    testImplementation(testFixtures(project(":modules:test-fixtures:hive-metastore")))
}
