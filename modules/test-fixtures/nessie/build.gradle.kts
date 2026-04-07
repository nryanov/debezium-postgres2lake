plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesApi(testFixtures(project(":modules:test-fixtures:common")))

    testFixturesApi(platform(libs.testcontainers.bom))
    testFixturesApi("org.testcontainers:testcontainers")
    testFixturesApi("io.quarkus:quarkus-junit")
    testFixturesImplementation(libs.testcontainers.nessie)
}
