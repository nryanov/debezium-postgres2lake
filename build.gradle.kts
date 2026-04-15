import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.plugins.signing.SigningExtension

plugins {
    java
}

group = "debezium-postgres2lake"
version = providers.gradleProperty("releaseVersion").orElse("0.1.0").get()

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

    // copy JIB scripts in each application module
    plugins.withId("io.quarkus") {
        val syncJibExtras = tasks.register<Sync>("syncJibExtras") {
            from(rootProject.layout.projectDirectory.dir("modules/jib/src/main/jib"))
            into(layout.projectDirectory.dir("src/main/jib"))
        }
        tasks.named("processResources").configure { dependsOn(syncJibExtras) }
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

    if (path.startsWith(":extensions:")) {
        apply(plugin = "maven-publish")
        apply(plugin = "signing")

        plugins.withType<JavaPlugin>().configureEach {
            extensions.configure<JavaPluginExtension> {
                withSourcesJar()
                withJavadocJar()
            }

            extensions.configure<PublishingExtension> {
                publications {
                    create<MavenPublication>("mavenJava") {
                        from(components["java"])
                        groupId = project.group.toString()
                        artifactId = project.name
                        version = project.version.toString()

                        pom {
                            name.set(project.name)
                            description.set(
                                "SPI extension module for debezium-postgres2lake: ${project.name}",
                            )
                            url.set("https://github.com/nryanov/debezium-postgres2lake")
                            licenses {
                                license {
                                    name.set("Apache License, Version 2.0")
                                    url.set("https://www.apache.org/licenses/LICENSE-2.0")
                                    distribution.set("repo")
                                }
                            }
                            developers {
                                developer {
                                    id.set("nryanov")
                                    name.set("Nikita Ryanov")
                                    url.set("https://github.com/nryanov")
                                }
                            }
                            scm {
                                url.set("https://github.com/nryanov/debezium-postgres2lake")
                                connection.set("scm:git:git://github.com/nryanov/debezium-postgres2lake.git")
                                developerConnection.set("scm:git:ssh://git@github.com/nryanov/debezium-postgres2lake.git")
                            }
                        }
                    }
                }
                repositories {
                    maven {
                        name = "mavenCentral"
                        url = uri(
                            providers.gradleProperty("mavenCentralPublishUrl")
                                .orElse("https://central.sonatype.com/repository/maven-releases/")
                                .get(),
                        )
                        credentials {
                            username = providers.gradleProperty("mavenCentralUsername").orNull
                            password = providers.gradleProperty("mavenCentralPassword").orNull
                        }
                    }
                }
            }

            extensions.configure<SigningExtension> {
                val signingKey = providers.gradleProperty("signingInMemoryKey").orNull
                val signingPassword = providers.gradleProperty("signingInMemoryKeyPassword").orNull
                useInMemoryPgpKeys(signingKey, signingPassword)
                sign(extensions.getByType(PublishingExtension::class.java).publications)
            }
        }
    }
}
