plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    implementation(project(":extensions:readiness-marker-event-emitter-api"))
    implementation(libs.slf4j)

    implementation(enforcedPlatform(libs.aws.platform))
    implementation("software.amazon.awssdk:aws-core")
    implementation("software.amazon.awssdk:regions")
    implementation("software.amazon.awssdk:auth")
    implementation("software.amazon.awssdk:sdk-core")
    implementation("software.amazon.awssdk:http-auth")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk:netty-nio-client")

    testImplementation(testFixtures(project(":modules:test-fixtures:common")))
}