plugins {
    id("java-platform")
    id("io.quarkus") version "3.31.2" apply false
}

allprojects {
    group = "com.nryanov.debezium.server.lake"
    version = "0.1.0"

    repositories {
        mavenCentral()
        maven {
            name = "Confluent"
            url = uri("https://packages.confluent.io/maven/")
        }
    }
}

dependencies {
    constraints {
        api(platform(libs.debezium.server.bom))
        api(platform(libs.debezium.bom))
        api(platform(libs.iceberg.bom))
        api(platform(libs.testcontainers.bom))
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.quarkus")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}