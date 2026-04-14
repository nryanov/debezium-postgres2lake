plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    implementation(project(":extensions:commit-event-emitter-api"))
    implementation(libs.kafka.client)
}