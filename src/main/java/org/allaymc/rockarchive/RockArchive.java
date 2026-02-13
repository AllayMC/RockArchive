package org.allaymc.rockarchive;

import org.allaymc.api.plugin.Plugin;
import org.allaymc.api.registry.Registries;
import org.rocksdb.RocksDB;

public class RockArchive extends Plugin {
    @Override
    public void onLoad() {
        RocksDB.loadLibrary();
        Registries.WORLD_STORAGE_FACTORIES.register("ROCKSDB", RocksDBWorldStorage::new);
        this.pluginLogger.info("RockArchive is loaded!");
    }

    @Override
    public void onEnable() {
        this.pluginLogger.info("RockArchive is enabled!");
    }

    @Override
    public void onDisable() {
        this.pluginLogger.info("RockArchive is disabled!");
    }
}