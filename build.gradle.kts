plugins {
    kotlin("jvm") version "1.9.0"
    id("org.flywaydb.flyway") version "8.5.0"
    
}


group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.postgresql:postgresql:42.3.3")
    implementation("org.flywaydb:flyway-core:8.5.0")
    
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.2")
    
    testImplementation("org.testcontainers:junit-jupiter:1.17.6")
    testImplementation("org.testcontainers:postgresql:1.17.6")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5:1.8.20")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
            events("passed", "skipped", "failed", "standardOut", "standardError")
            showExceptions = true
            showCauses = true
            showStackTraces = true
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showStandardStreams = true
        }
    
}

flyway {
    url = "jdbc:postgresql://localhost:5432/testdb"
    user = "testuser"
    password = "testpassword"
    locations = arrayOf("filesystem:src/main/resources/db/migration")
}