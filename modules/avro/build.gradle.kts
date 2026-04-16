plugins {
    java
    id("io.quarkus")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation(project(":modules:jib"))
    implementation(project(":modules:domain"))
    implementation(project(":modules:core"))

    implementation(libs.codec.xz)

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
    testImplementation(testFixtures(project(":modules:test-fixtures:s3")))
    testImplementation(testFixtures(project(":modules:test-fixtures:postgres")))
    testImplementation(testFixtures(project(":modules:test-fixtures:schema-registry")))
}
