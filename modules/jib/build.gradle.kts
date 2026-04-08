plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    implementation(project(":modules:platform"))
    implementation("io.quarkus:quarkus-container-image-jib")
}
