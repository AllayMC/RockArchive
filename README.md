# RockArchive

RockArchive is a high-performance world storage plugin for [AllayMC](https://github.com/AllayMC/Allay) powered by [RocksDB](https://rocksdb.org/).

It replaces traditional LevelDB-based world storage with a modern, highly configurable backend that offers improved compression efficiency, better
write concurrency, and scalable compaction strategies for large-scale servers.

## Features

- **ZSTD Compression** — Uses ZSTD for both regular and bottommost-level compaction, delivering better compression ratios than LevelDB's default Snappy/Zlib.
- **Bloom Filter** — 10-bit Bloom filter on data blocks to speed up point lookups (`containChunk`, entity reads, etc.).
- **Concurrent Background Jobs** — Automatically scales background flush/compaction threads to half the available CPU cores.
- **Drop-in Replacement** — Registers as a `ROCKSDB` world storage factory; just set the storage type in your world configuration.

## Installation

1. Download the latest release JAR from [Releases](https://github.com/AllayMC/RockArchive/releases).
2. Place the JAR in your AllayMC server's `plugins/` directory.
3. Edit `worlds/world-settings.yml`, set `storage-type` to `ROCKSDB`:

```yaml
worlds:
  world:
    storage-type: ROCKSDB
```

4. Restart the server.

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.