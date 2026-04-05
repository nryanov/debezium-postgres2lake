plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesApi(testFixtures(project(":modules:test-fixtures:common")))

    testFixturesApi("org.testcontainers:minio")
    testFixturesApi("org.testcontainers:testcontainers")
    testFixturesApi("io.quarkus:quarkus-junit")

    testFixturesImplementation("software.amazon.awssdk:aws-core")
    testFixturesImplementation("software.amazon.awssdk:regions")
    testFixturesImplementation("software.amazon.awssdk:auth")
    testFixturesImplementation("software.amazon.awssdk:sdk-core")
    testFixturesImplementation("software.amazon.awssdk:http-auth")
    testFixturesImplementation("software.amazon.awssdk:s3-transfer-manager")
    testFixturesImplementation("software.amazon.awssdk:netty-nio-client")
}
