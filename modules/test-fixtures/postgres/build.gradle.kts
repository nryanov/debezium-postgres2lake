plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesApi(testFixtures(project(":modules:test-fixtures:common")))

    testFixturesApi("org.testcontainers:postgresql")
    testFixturesApi("org.testcontainers:testcontainers")
    testFixturesApi("io.quarkus:quarkus-junit")
}
