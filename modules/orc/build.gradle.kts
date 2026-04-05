plugins {
    java
    id("io.quarkus")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation(project(":modules:domain"))
    implementation(project(":modules:core"))

    implementation(libs.orc.core) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation("org.apache.hive:hive-exec:4.2.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "asm")
        exclude(group = "org.apache.logging.log4j")
    }

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
    testImplementation(testFixtures(project(":modules:test-fixtures:s3")))
    testImplementation(testFixtures(project(":modules:test-fixtures:postgres")))
    testImplementation(testFixtures(project(":modules:test-fixtures:schema-registry")))
}
