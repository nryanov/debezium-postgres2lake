plugins {
    java
    id("io.quarkus")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation(project(":modules:domain"))
    implementation(project(":modules:core"))

    implementation(libs.parquet.avro) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
    testImplementation(testFixtures(project(":modules:test-fixtures:s3")))
    testImplementation(testFixtures(project(":modules:test-fixtures:postgres")))
    testImplementation(testFixtures(project(":modules:test-fixtures:schema-registry")))
}
