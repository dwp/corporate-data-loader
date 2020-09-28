plugins {
    java
    kotlin("jvm") version "1.4.10"
    application
    id("com.github.johnrengelman.shadow") version "5.1.0"
}

group = "corporate-data-loader"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.apache.hbase:hbase-client:1.4.13")
    implementation("org.apache.hbase:hbase-server:1.4.13")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.701")
    implementation("com.amazonaws:aws-java-sdk-core:1.11.701")
    implementation("com.amazonaws:aws-java-sdk-secretsmanager:1.11.819")
}



configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

application {
    mainClassName = "app.load.LoadKt"
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
}
