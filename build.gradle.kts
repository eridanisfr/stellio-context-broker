import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

plugins {
    id("org.springframework.boot") version "2.1.6.RELEASE"
    id("io.spring.dependency-management") version "1.0.7.RELEASE"
    kotlin("jvm") version "1.3.11"
    kotlin("plugin.spring") version "1.3.11"
    kotlin("plugin.noarg") version "1.3.11"
    id("org.jlleitschuh.gradle.ktlint") version "8.2.0"
}

group = "com.egm.datahub"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_1_8

val developmentOnly by configurations.creating
configurations {
    runtimeClasspath {
        extendsFrom(developmentOnly)
    }
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
    jcenter()
}

dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES) {
            bomProperty("kotlin.version", "1.3.11")
        }
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // implementation("org.springframework.boot:spring-boot-starter-oauth2-client")
    // implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    // implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.eclipse.rdf4j:rdf4j-rio-jsonld:2.5.3")
    implementation("com.google.code.gson:gson:2.8.5")
    implementation("org.neo4j:neo4j-ogm-core:3.1.14")
    implementation("org.neo4j:neo4j-ogm-api:3.1.14")
    implementation("org.neo4j:neo4j-ogm-bolt-driver:3.1.14")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(module = "junit")
        exclude(module = "mockito-core")
        exclude(group = "org.hamcrest")
    }
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.hamcrest:hamcrest:2.1")
    testImplementation("com.ninja-squad:springmockk:1.1.2")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.neo4j:neo4j-ogm-embedded-driver:3.1.14")
    testImplementation("org.neo4j:neo4j:3.4.15")
    testImplementation("org.neo4j:neo4j-bolt:3.4.15")
    // testImplementation("org.springframework.security:spring-security-test")
    testImplementation("com.intuit.karate:karate-junit5:0.9.4")
    testImplementation("com.intuit.karate:karate-apache:0.9.4")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

defaultTasks("bootRun")

ktlint {
    verbose.set(true)
    outputToConsole.set(true)
    coloredOutput.set(true)
    reporters.set(setOf(ReporterType.CHECKSTYLE, ReporterType.JSON))
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "1.8"
    }
}

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

sourceSets {
    test {
        resources {
            srcDir(file("src/test/kotlin"))
            exclude("**/*.kt")
        }
    }
}

tasks.withType<Test> {
    environment("SPRING_PROFILES_ACTIVE", "test")
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }

    // pull karate options into the runtime
    systemProperties["karate.options"] = System.getProperties().getProperty("karate.options")
    // pull karate env into the runtime
    systemProperties["karate.env"] = System.getProperties().getProperty("karate.env")
    // ensure tests are always run
    outputs.upToDateWhen { false }
}
