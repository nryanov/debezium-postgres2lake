plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    implementation(project(":extensions:data-catalog-api"))
    implementation(libs.datahub.client)
    implementation(libs.slf4j)
}