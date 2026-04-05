plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesApi(project(":modules:platform"))
    testFixturesApi(project(":modules:domain"))

    testFixturesApi(platform(libs.testcontainers.bom))
    testFixturesApi(platform(libs.junit.bom))

    testFixturesRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testFixturesRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testFixturesApi("org.junit.jupiter:junit-jupiter-api")
    testFixturesApi(libs.awaitility)
}
