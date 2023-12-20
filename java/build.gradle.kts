plugins {
    kotlin("jvm") version Version.KOTLIN
}

val projectGroup = "kr.jclab.grpcover"
val projectVersion = Version.PROJECT

group = projectGroup
version = projectVersion

allprojects {
    group = projectGroup
    version = projectVersion

    repositories {
        mavenCentral()
    }
}

dependencies {}
