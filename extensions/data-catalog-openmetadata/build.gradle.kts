plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    implementation(project(":extensions:data-catalog-api"))
    implementation(libs.openmetadata.java.client)
}