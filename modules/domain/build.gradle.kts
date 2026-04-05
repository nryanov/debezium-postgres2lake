plugins {
    `java-library`
    id("org.kordamp.gradle.jandex")
}

dependencies {
    api(libs.confluent.avro)
}