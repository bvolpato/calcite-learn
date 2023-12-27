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

val arrowVersion = "14.0.1"
val adbcVersion = "0.8.0"
val calciteVersion = "1.36.0"
val substraitVersion = "0.23.0"

dependencies {

    // Arrow
    implementation("org.apache.arrow:arrow-vector:${arrowVersion}")
    implementation("org.apache.arrow:arrow-memory-netty:${arrowVersion}")
    implementation("org.apache.arrow:arrow-algorithm:${arrowVersion}")
    implementation("org.apache.arrow:arrow-dataset:${arrowVersion}")
    implementation("org.apache.arrow.adbc:adbc-driver-jdbc:${adbcVersion}")
    implementation("io.substrait:core:${substraitVersion}")
    implementation("io.substrait:isthmus:${substraitVersion}")

    // Calcite
    implementation("org.apache.calcite:calcite-core:${calciteVersion}")
    implementation("org.xerial:sqlite-jdbc:3.44.0.0")

    // DuckDB
    implementation("org.duckdb:duckdb_jdbc:0.9.2")

    // Spark
    implementation("org.apache.spark:spark-core_2.13:3.5.0")
    implementation("org.apache.spark:spark-sql_2.13:3.5.0")

    // Postgres
    implementation("org.postgresql:postgresql:42.7.1")

    // Misc
    implementation("com.google.guava:guava:32.1.1-jre")
    implementation("org.brunocvcunha.inutils4j:inutils4j:0.8")
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