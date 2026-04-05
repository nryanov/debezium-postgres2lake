import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension

plugins {
    java
}

group = "debezium-postgres2lake"
version = "0.1.0"

subprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
        mavenLocal()
        maven {
            url = uri("https://packages.confluent.io/maven")
        }
    }

    plugins.withType<JavaPlugin>().configureEach {
        extensions.configure<JavaPluginExtension> {
            sourceCompatibility = JavaVersion.VERSION_21
            targetCompatibility = JavaVersion.VERSION_21
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
        jvmArgs(
            "--add-opens", "java.base/java.io=ALL-UNNAMED",
            "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens", "java.base/java.lang=ALL-UNNAMED",
            "--add-opens", "java.base/java.math=ALL-UNNAMED",
            "--add-opens", "java.base/java.net=ALL-UNNAMED",
            "--add-opens", "java.base/java.nio=ALL-UNNAMED",
            "--add-opens", "java.base/java.text=ALL-UNNAMED",
            "--add-opens", "java.base/java.time=ALL-UNNAMED",
            "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens", "java.base/java.util.regex=ALL-UNNAMED",
            "--add-opens", "java.base/java.util=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.reflect=ALL-UNNAMED",
            "--add-opens", "java.sql/java.sql=ALL-UNNAMED",
            "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
            "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
            "-Djava.security.manager=allow"
        )
    }

    // Keep aligned with [versions].confluent in gradle/libs.versions.toml
    val confluentAlignVersion = "8.1.1"
    configurations.configureEach {
        resolutionStrategy {
            force(
                "io.confluent:kafka-connect-avro-converter:$confluentAlignVersion",
                "io.confluent:kafka-connect-avro-data:$confluentAlignVersion",
                "io.confluent:kafka-avro-serializer:$confluentAlignVersion",
                "io.confluent:kafka-schema-registry-client:$confluentAlignVersion",
                "io.confluent:kafka-schema-serializer:$confluentAlignVersion",
                "io.confluent:common-utils:$confluentAlignVersion",
            )
        }
    }
}
