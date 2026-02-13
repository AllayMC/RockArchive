plugins {
    id("java-library")
    id("org.allaymc.gradle.plugin") version "0.2.1"
}

group = "org.allaymc.rockarchive"
description = "RockArchive is a high-performance world storage format for AllayMC powered by RocksDB"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

allay {
    api = "0.25.0"
    apiOnly = false

    plugin {
        entrance = ".RockArchive"
        authors += "daoge_cmd"
        website = "https://github.com/AllayMC/RockArchive"
    }
}

dependencies {
    compileOnly(group = "org.projectlombok", name = "lombok", version = "1.18.34")
    implementation(group = "org.rocksdb", name = "rocksdbjni", version = "10.2.1")
    annotationProcessor(group = "org.projectlombok", name = "lombok", version = "1.18.34")
}
