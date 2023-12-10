import org.gradle.api.tasks.JavaExec

plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application

    id("com.diffplug.spotless") version "6.23.3"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {

    // Arrow
    implementation("org.apache.arrow:arrow-vector:14.0.1")
    implementation("org.apache.arrow:arrow-memory-netty:14.0.1")
    implementation("org.apache.arrow:arrow-algorithm:14.0.1")
    implementation("org.apache.arrow:arrow-dataset:14.0.1")
    implementation("io.substrait:core:0.22.0")
    implementation("io.substrait:isthmus:0.22.0")

    // Calcite
    implementation("org.apache.calcite:calcite-core:1.36.0")
    implementation("org.xerial:sqlite-jdbc:3.44.0.0")


    implementation("com.google.guava:guava:32.1.1-jre")

    testImplementation("junit:junit:4.13.2")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    // Define the main class for the application.
    mainClass.set("org.bvolpato.query.App")
}

tasks.withType<JavaExec> {
    if (name == "run") {
        jvmArgs("--add-opens", "java.base/jdk.internal.loader=ALL-UNNAMED")
    }
}


spotless {
    java {
        googleJavaFormat("1.18.1")
        formatAnnotations()
    }
}