import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.google.protobuf.gradle.*

plugins {
    id("java")
    id("org.springframework.boot") version "2.6.6"
    id("io.spring.dependency-management") version "1.1.4"
    kotlin("jvm")
    kotlin("plugin.spring") version "1.9.20"
    id("com.google.protobuf") version "0.9.4"
}

group = "kr.jclab.grpcover"
version = "0.0.1-rc10"

repositories {
    mavenCentral()
}

val nettyVersion = "4.1.104.Final"
val projectGrpcVersion = "1.60.0"
val projectProtobufVersion = "3.25.1"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("kr.jclab.netty:netty-pseudo-channel:0.0.1-rc3")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.security:spring-security-test")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")

    implementation("io.netty:netty-buffer:${nettyVersion}")
    implementation("io.netty:netty-codec:${nettyVersion}")
    implementation("io.netty:netty-common:${nettyVersion}")
    implementation("io.netty:netty-handler:${nettyVersion}")
    implementation("io.netty:netty-codec-http:${nettyVersion}")
    implementation("io.netty:netty-codec-http2:${nettyVersion}")
    implementation("com.google.protobuf:protobuf-java:${projectProtobufVersion}")
    implementation("io.grpc:grpc-protobuf:${projectGrpcVersion}")
    implementation("io.grpc:grpc-stub:${projectGrpcVersion}")
    implementation("io.grpc:grpc-core:${projectGrpcVersion}")
    implementation("io.grpc:grpc-netty:${projectGrpcVersion}")

    implementation(project(":core"))
    implementation(project(":websocket"))
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${projectProtobufVersion}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${projectGrpcVersion}"
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
//    generateProtoTasks {
//        all().forEach {
//            it.plugins {
//                id("grpc")
//                id("grpckt")
//            }
//            it.builtins {
//                id("kotlin")
//            }
//        }
//    }
}
//
//sourceSets {
//    main {
//        java {
//            srcDirs 'build/generated/source/proto/main/grpc'
//            srcDirs 'build/generated/source/proto/main/java'
//        }
//    }
//    test {
//        java {
//            srcDirs 'build/generated/source/proto/test/grpc'
//            srcDirs 'build/generated/source/proto/test/java'
//        }
//    }
//}
