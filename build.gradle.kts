plugins {
    kotlin("jvm") version "1.9.23"
    kotlin("plugin.serialization") version "1.6.0"

}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.mongodb:mongodb-driver-sync:4.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")
    implementation ("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}