plugins {
    kotlin("jvm") version "2.1.10"
}

group = "no.nav.fagtorsdag"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val kafkaVersion = "3.9.0"
val kotestVersion = "5.9.1"
val jacksonVersion = "2.18.3"
dependencies {
    implementation("org.slf4j:slf4j-simple:1.7.32")
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-core-jvm:$kotestVersion")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}