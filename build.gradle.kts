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
    jcenter()
    maven(url = "https://jitpack.io")
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.4.0")
    implementation("org.apache.hbase:hbase-client:1.4.13")
    implementation("org.apache.hbase:hbase-server:1.4.13")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.701")
    implementation("com.amazonaws:aws-java-sdk-core:1.11.701")
    implementation("com.amazonaws:aws-java-sdk-secretsmanager:1.11.819")
    implementation("com.beust:klaxon:4.0.2")
    implementation("mysql:mysql-connector-java:6.0.6")
    implementation("com.github.dwp:dataworks-common-logging:0.0.5")

    testImplementation("org.junit.jupiter:junit-jupiter:5.4.2")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:4.2.0")
    testImplementation("io.kotest:kotest-assertions-core-jvm:4.2.0")
    testImplementation("io.kotest:kotest-property:4.2.0")
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")
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

configurations.all {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
