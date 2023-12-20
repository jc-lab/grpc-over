import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.google.protobuf.gradle.*

plugins {
    `java-library`
    `maven-publish`
    `signing`
    id("com.google.protobuf") version "0.9.4"
}

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}

dependencies {
    if (JavaVersion.current().isJava9Compatible()) {
        // Workaround for @javax.annotation.Generated
        // see: https://github.com/grpc/grpc-java/issues/3633
        implementation("javax.annotation:javax.annotation-api:1.3.2")
    }

    compileOnly("org.projectlombok:lombok:1.18.22")
    annotationProcessor("org.projectlombok:lombok:1.18.22")

    testCompileOnly("org.projectlombok:lombok:1.18.22")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.22")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.mockito:mockito-core:3.4.0")
    testImplementation("com.google.truth:truth:1.1.3")
    testImplementation("io.grpc:grpc-testing:${Version.GRPC}")
    testImplementation("io.grpc:grpc-testing-proto:${Version.GRPC}")
    testImplementation("com.google.guava:guava-testlib:31.0.1-android")
    testImplementation("io.perfmark:perfmark-api:0.25.0")
    testImplementation("org.slf4j:slf4j-simple:2.0.0-alpha7")

    implementation("org.slf4j:slf4j-api:2.0.0-alpha7")

    api("com.google.protobuf:protobuf-java:${Version.PROTOBUF}")
    api("io.grpc:grpc-protobuf:${Version.GRPC}")
    api("io.grpc:grpc-stub:${Version.GRPC}")
    api("io.grpc:grpc-core:${Version.GRPC}")
    api("io.grpc:grpc-netty:${Version.GRPC}")

    implementation("io.perfmark:perfmark-api:0.26.0")

    api("io.netty:netty-buffer:${Version.NETTY}")
    api("io.netty:netty-codec:${Version.NETTY}")
    api("io.netty:netty-common:${Version.NETTY}")
    api("io.netty:netty-handler:${Version.NETTY}")
    api("kr.jclab.netty:netty-pseudo-channel:0.0.1-rc2")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${Version.PROTOBUF}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${Version.GRPC}"
        }
    }

    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
            }
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set(project.name)
                description.set("GRPC over Anything")
                url.set("https://github.com/jc-lab/grpc-over")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("jclab")
                        name.set("Joseph Lee")
                        email.set("joseph@jc-lab.net")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/jc-lab/grpc-over.git")
                    developerConnection.set("scm:git:ssh://git@github.com/jc-lab/grpc-over.git")
                    url.set("https://github.com/jc-lab/grpc-over")
                }
            }
        }
    }
    repositories {
        maven {
            val releasesRepoUrl = "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            val snapshotsRepoUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots/"
            url = uri(if ("$version".endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
            credentials {
                username = findProperty("ossrhUsername") as String?
                password = findProperty("ossrhPassword") as String?
            }
        }
    }
}

signing {
    useGpgCmd()
    sign(publishing.publications)
}

tasks.withType<Sign>().configureEach {
    onlyIf { project.hasProperty("signing.gnupg.keyName") || project.hasProperty("signing.keyId") }
}
