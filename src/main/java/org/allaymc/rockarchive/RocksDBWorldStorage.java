package org.allaymc.rockarchive;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.allaymc.api.entity.Entity;
import org.allaymc.api.server.Server;
import org.allaymc.api.utils.AllayNBTUtils;
import org.allaymc.api.utils.NBTIO;
import org.allaymc.api.world.World;
import org.allaymc.api.world.WorldData;
import org.allaymc.api.world.chunk.Chunk;
import org.allaymc.api.world.chunk.ChunkState;
import org.allaymc.api.world.chunk.OperationType;
import org.allaymc.api.world.data.DimensionInfo;
import org.allaymc.api.world.storage.WorldStorage;
import org.allaymc.api.world.storage.WorldStorageException;
import org.allaymc.server.world.AllayWorldData;
import org.allaymc.server.world.chunk.AllayChunkSection;
import org.allaymc.server.world.chunk.AllayUnsafeChunk;
import org.allaymc.server.world.storage.leveldb.LevelDBUtils;
import org.allaymc.server.world.storage.leveldb.codec.BlockEntityCodec;
import org.allaymc.server.world.storage.leveldb.codec.ChunkSectionCodec;
import org.allaymc.server.world.storage.leveldb.codec.HeightAndBiomeCodec;
import org.allaymc.server.world.storage.leveldb.codec.ScheduledUpdateCodec;
import org.allaymc.server.world.storage.leveldb.codec.WorldDataCodec;
import org.allaymc.server.world.storage.leveldb.data.ChunkVersion;
import org.allaymc.server.world.storage.leveldb.data.LevelDBKey;
import org.allaymc.server.world.storage.leveldb.data.VanillaChunkState;
import org.cloudburstmc.nbt.NBTInputStream;
import org.cloudburstmc.nbt.NbtMap;
import org.cloudburstmc.nbt.NbtUtils;
import org.rocksdb.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * RocksDB implementation of {@link WorldStorage} which replaces LevelDB usage with RocksDB (rocksdbjni).
 *
 * @author daoge_cmd
 */
@Slf4j
public class RocksDBWorldStorage implements WorldStorage {

    private static final String FILE_LEVEL_DAT = "level.dat";
    private static final String FILE_LEVEL_DAT_OLD = "level.dat_old";

    private static final String DIR_DB = "db";

    private static final int CURRENT_CHUNK_VERSION = ChunkVersion.V1_21_40.ordinal();

    private final Path path;
    private final String worldName;

    // RocksDB objects
    private final RocksDB db;
    private final Options options;

    private World world;

    public RocksDBWorldStorage(Path path) {
        this.path = path;
        this.worldName = path.getName(path.getNameCount() - 1).toString();

        var file = path.toFile();
        if (!file.exists() && !file.mkdirs()) {
            throw new WorldStorageException("Failed to create world directory!");
        }

        var dbFolder = path.resolve(DIR_DB).toFile();
        if (!dbFolder.exists() && !dbFolder.mkdirs()) {
            throw new WorldStorageException("Failed to create world database directory!");
        }

        try {
            // Build a recommended default Options
            var tableConfig = new BlockBasedTableConfig()
                    .setBlockSize(64 * 1024) // 64KB block
                    .setCacheIndexAndFilterBlocks(true)
                    .setFilterPolicy(new BloomFilter(10, false));

            this.options = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    // Use ZSTD for overall compression and bottommost
                    .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                    // concurrency hints
                    .setMaxBackgroundJobs(Math.max(2, Runtime.getRuntime().availableProcessors() / 2))
                    .setWriteBufferSize(64 * 1024 * 1024)
                    .setTargetFileSizeBase(64 * 1024 * 1024);

            this.db = RocksDB.open(this.options, dbFolder.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new WorldStorageException(e);
        }
    }

    @Override
    public void setWorld(World world) {
        Preconditions.checkState(this.world == null, "World has already been set");
        this.world = world;
    }

    @Override
    public CompletableFuture<Chunk> readChunk(int chunkX, int chunkZ, DimensionInfo dimensionInfo) {
        return CompletableFuture
                .supplyAsync(() -> readChunkSync(chunkX, chunkZ, dimensionInfo), Server.getInstance().getVirtualThreadPool())
                .exceptionally(t -> {
                    log.error("Failed to read chunk ({}, {})", chunkX, chunkZ, t);
                    return AllayUnsafeChunk.builder().newChunk(chunkX, chunkZ, dimensionInfo).toSafeChunk();
                });
    }

    @SneakyThrows
    @Override
    public Chunk readChunkSync(int chunkX, int chunkZ, DimensionInfo dimensionInfo) {
        var builder = AllayUnsafeChunk.builder()
                .chunkX(chunkX)
                .chunkZ(chunkZ)
                .dimensionInfo(dimensionInfo)
                .state(ChunkState.NEW);
        if (!containChunk(chunkX, chunkZ, dimensionInfo)) {
            return builder.build().toSafeChunk();
        }

        var versionValue = db.get(LevelDBKey.VERSION.createKey(chunkX, chunkZ, dimensionInfo));
        if (versionValue == null) {
            versionValue = db.get(LevelDBKey.LEGACY_VERSION.createKey(chunkX, chunkZ, dimensionInfo));
        }
        if (versionValue == null) {
            log.warn("Chunk at ({}, {}) is missing version field", chunkX, chunkZ);
        }

        var chunkFinalizedState = db.get(LevelDBKey.CHUNK_FINALIZED_STATE.createKey(chunkX, chunkZ, dimensionInfo));
        if (chunkFinalizedState != null && Unpooled.wrappedBuffer(chunkFinalizedState).readByte() != VanillaChunkState.DONE.ordinal()) {
            // Older versions didn't have CHUNK_FINALIZED_STATE data, so we still load this chunk
            return builder.build().toSafeChunk();
        }

        var chunkStateBytes = db.get(LevelDBKey.ALLAY_CHUNK_STATE.createKey(chunkX, chunkZ, dimensionInfo));
        if (chunkStateBytes != null) {
            var chunkStateName = new String(chunkStateBytes, StandardCharsets.UTF_8);
            try {
                builder.state(ChunkState.valueOf(chunkStateName));
            } catch (IllegalArgumentException e) {
                log.warn("Unknown chunk state {} for chunk at ({}, {})!", chunkStateName, chunkX, chunkZ);
                builder.state(ChunkState.FULL);
            }
        } else {
            builder.state(ChunkState.FULL);
        }

        // Sections
        var sections = new AllayChunkSection[dimensionInfo.chunkSectionCount()];
        var minSectionY = dimensionInfo.minSectionY();
        for (int sectionY = minSectionY; sectionY <= dimensionInfo.maxSectionY(); sectionY++) {
            var sectionData = db.get(LevelDBKey.CHUNK_SECTION_PREFIX.createKey(chunkX, chunkZ, sectionY, dimensionInfo));
            if (sectionData != null) {
                sections[sectionY - minSectionY] = ChunkSectionCodec.deserialize(sectionData, sectionY, chunkX, chunkZ);
            }
        }
        builder.sections(ChunkSectionCodec.fillNullSections(sections, dimensionInfo));

        // Height and biome
        var data3d = db.get(LevelDBKey.DATA_3D.createKey(chunkX, chunkZ, dimensionInfo));
        if (data3d != null) {
            HeightAndBiomeCodec.deserialize(data3d, builder);
        } else {
            var data2d = db.get(LevelDBKey.DATA_2D.createKey(chunkX, chunkZ, dimensionInfo));
            if (data2d != null) {
                HeightAndBiomeCodec.deserializeOld(data2d, builder);
            }
        }

        // Block entities
        var tileBytes = db.get(LevelDBKey.BLOCK_ENTITIES.createKey(chunkX, chunkZ, dimensionInfo));
        if (tileBytes != null) {
            builder.blockEntities(BlockEntityCodec.deserialize(tileBytes, world, dimensionInfo));
        }

        // Scheduled updates
        var scheduledBytes = db.get(LevelDBKey.PENDING_TICKS.createKey(chunkX, chunkZ, dimensionInfo));
        if (scheduledBytes != null) {
            builder.scheduledUpdates(ScheduledUpdateCodec.deserialize(scheduledBytes));
        }

        return builder.build().toSafeChunk();
    }

    @Override
    public CompletableFuture<Void> writeChunk(Chunk chunk) {
        return CompletableFuture
                .runAsync(() -> writeChunkSync(chunk), Server.getInstance().getVirtualThreadPool())
                .exceptionally(t -> {
                    log.error("Failed to write chunk ({}, {})", chunk.getX(), chunk.getZ(), t);
                    return null;
                });
    }

    @Override
    public void writeChunkSync(Chunk chunk) {
        try (var writeBatch = new WriteBatch();
             var writeOptions = new WriteOptions()) {

            // Version
            writeBatch.put(
                    LevelDBKey.VERSION.createKey(chunk.getX(), chunk.getZ(), chunk.getDimensionInfo()),
                    new byte[]{(byte) CURRENT_CHUNK_VERSION}
            );

            // finalized state
            writeBatch.put(
                    LevelDBKey.CHUNK_FINALIZED_STATE.createKey(chunk.getX(), chunk.getZ(), chunk.getDimensionInfo()),
                    LevelDBUtils.withByteBufToArray(buf -> buf.writeByte(VanillaChunkState.DONE.ordinal()))
            );

            // chunk state
            writeBatch.put(
                    LevelDBKey.ALLAY_CHUNK_STATE.createKey(chunk.getX(), chunk.getZ(), chunk.getDimensionInfo()),
                    chunk.getState().name().getBytes()
            );

            chunk.applyOperation(c -> writeChunk0((AllayUnsafeChunk) c, writeBatch), OperationType.READ, OperationType.READ);

            // write to DB
            db.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new WorldStorageException(e);
        }
    }

    @SneakyThrows
    protected void writeChunk0(AllayUnsafeChunk chunk, WriteBatch writeBatch) {
        var dimensionInfo = chunk.getDimensionInfo();

        // Sections
        for (int ySection = dimensionInfo.minSectionY(); ySection <= dimensionInfo.maxSectionY(); ySection++) {
            var section = chunk.getSection(ySection);
            if (!section.hasDirtyBlockLayer()) {
                continue;
            }
            writeBatch.put(
                    LevelDBKey.CHUNK_SECTION_PREFIX.createKey(chunk.getX(), chunk.getZ(), ySection, dimensionInfo),
                    ChunkSectionCodec.serialize(section, ySection)
            );
        }

        // Height and biome
        writeBatch.put(
                LevelDBKey.DATA_3D.createKey(chunk.getX(), chunk.getZ(), dimensionInfo),
                HeightAndBiomeCodec.serialize(chunk)
        );

        // Block entities
        var blockEntitiesKey = LevelDBKey.BLOCK_ENTITIES.createKey(chunk.getX(), chunk.getZ(), dimensionInfo);
        var blockEntitiesData = BlockEntityCodec.serialize(chunk.getBlockEntities().values());
        if (blockEntitiesData == null) {
            writeBatch.delete(blockEntitiesKey);
        } else {
            writeBatch.put(blockEntitiesKey, blockEntitiesData);
        }

        // Scheduled updates
        var scheduledKey = LevelDBKey.PENDING_TICKS.createKey(chunk.getX(), chunk.getZ(), dimensionInfo);
        var scheduledData = ScheduledUpdateCodec.serialize(chunk.getScheduledUpdates().values(), world.getTick());
        if (scheduledData == null) {
            writeBatch.delete(scheduledKey);
        } else {
            writeBatch.put(scheduledKey, scheduledData);
        }
    }

    @Override
    public CompletableFuture<Map<Long, Entity>> readEntities(int chunkX, int chunkZ, DimensionInfo dimensionInfo) {
        return CompletableFuture
                .supplyAsync(() -> readEntitiesSync(chunkX, chunkZ, dimensionInfo), Server.getInstance().getVirtualThreadPool())
                .exceptionally(t -> {
                    log.error("Failed to read entities in chunk ({}, {})", chunkX, chunkZ, t);
                    return Collections.emptyMap();
                });
    }

    @SneakyThrows
    @Override
    public Map<Long, Entity> readEntitiesSync(int chunkX, int chunkZ, DimensionInfo dimensionInfo) {
        var ids = db.get(LevelDBKey.createEntityIdsKey(chunkX, chunkZ, dimensionInfo));
        if (ids == null) {
            // Try to load entities through the old method
            return readEntitiesOldSync(chunkX, chunkZ, dimensionInfo);
        }

        var map = new Long2ObjectOpenHashMap<Entity>();
        var idsBuf = Unpooled.wrappedBuffer(ids);
        for (var i = 0; i < ids.length; i += Long.BYTES) {
            var id = idsBuf.readLongLE();
            var nbt = db.get(LevelDBKey.indexEntity(id));
            if (nbt == null) {
                log.error("NBT data for existing entity unique id {} is missing!", id);
                continue;
            }

            var entity = NBTIO.getAPI().fromEntityNBT(world.getDimension(dimensionInfo.dimensionId()), AllayNBTUtils.bytesToNbtLE(nbt));
            if (entity == null) {
                log.error("Failed to load entity from NBT {} in chunk ({}, {})", nbt, chunkX, chunkZ);
                continue;
            }

            map.put(entity.getUniqueId().getLeastSignificantBits(), entity);
        }

        return map;
    }

    @SneakyThrows
    protected Map<Long, Entity> readEntitiesOldSync(int chunkX, int chunkZ, DimensionInfo dimensionInfo) {
        var entityBytes = db.get(LevelDBKey.ENTITIES.createKey(chunkX, chunkZ, dimensionInfo));
        if (entityBytes == null) {
            return Collections.emptyMap();
        }

        var map = new Long2ObjectOpenHashMap<Entity>();
        for (var nbt : AllayNBTUtils.bytesToNbtListLE(entityBytes)) {
            var entity = NBTIO.getAPI().fromEntityNBT(world.getDimension(dimensionInfo.dimensionId()), nbt);
            if (entity == null) {
                log.error("Failed to load entity from NBT {} in chunk ({}, {})", nbt, chunkX, chunkZ);
                continue;
            }

            map.put(entity.getUniqueId().getLeastSignificantBits(), entity);
        }

        return map;
    }

    @Override
    public CompletableFuture<Void> writeEntities(int chunkX, int chunkZ, DimensionInfo dimensionInfo, Map<Long, Entity> entities) {
        return writeEntities0(chunkX, chunkZ, dimensionInfo, entities, true);
    }

    @Override
    public void writeEntitiesSync(int chunkX, int chunkZ, DimensionInfo dimensionInfo, Map<Long, Entity> entities) {
        writeEntities0(chunkX, chunkZ, dimensionInfo, entities, false);
    }

    protected CompletableFuture<Void> writeEntities0(int chunkX, int chunkZ, DimensionInfo dimensionInfo, Map<Long, Entity> entities, boolean asyncWrite) {
        var idsBuf = ByteBufAllocator.DEFAULT.buffer();
        // We create WriteBatch here and pass ownership to handler which will close it after write.
        var writeBatch = new WriteBatch();
        try {
            var idsKey = LevelDBKey.createEntityIdsKey(chunkX, chunkZ, dimensionInfo);

            // Delete the old entities in this chunk
            var oldIds = db.get(idsKey);
            if (oldIds != null) {
                var oldIdsBuf = Unpooled.wrappedBuffer(oldIds);
                for (var i = 0; i < oldIds.length; i += Long.BYTES) {
                    writeBatch.delete(LevelDBKey.indexEntity(oldIdsBuf.readLongLE()));
                }
            }

            // Write the new entities
            for (var entry : entities.entrySet()) {
                var entity = entry.getValue();
                idsBuf.writeLongLE(entry.getKey());
                writeBatch.put(LevelDBKey.indexEntity(entry.getKey()), AllayNBTUtils.nbtToBytesLE(entity.saveNBT()));
            }

            writeBatch.put(idsKey, ByteBufUtil.getBytes(idsBuf));
            return handleEntitiesWriteBatch(chunkX, chunkZ, writeBatch, asyncWrite);
        } catch (RocksDBException e) {
            throw new WorldStorageException(e);
        } finally {
            idsBuf.release();
        }
    }

    /**
     * This method takes ownership of the provided WriteBatch and is responsible for closing it.
     * If asyncWrite == true the actual RocksDB write will happen on the virtual thread pool.
     */
    protected CompletableFuture<Void> handleEntitiesWriteBatch(int chunkX, int chunkZ, WriteBatch writeBatch, boolean asyncWrite) {
        if (asyncWrite) {
            return CompletableFuture
                    .runAsync(() -> {
                        try (writeBatch; var writeOptions = new WriteOptions()) {
                            db.write(writeOptions, writeBatch);
                        } catch (RocksDBException e) {
                            throw new WorldStorageException(e);
                        }
                    }, Server.getInstance().getVirtualThreadPool())
                    .exceptionally(t -> {
                        log.error("Failed to write entities in chunk ({}, {})", chunkX, chunkZ, t);
                        return null;
                    });
        }

        try (writeBatch; var writeOptions = new WriteOptions()) {
            db.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new WorldStorageException(e);
        }

        return CompletableFuture.completedFuture(null);
    }

    @SneakyThrows
    @Override
    public boolean containChunk(int x, int z, DimensionInfo dimensionInfo) {
        for (int ySection = dimensionInfo.minSectionY(); ySection <= dimensionInfo.maxSectionY(); ySection++) {
            var bytes = db.get(LevelDBKey.CHUNK_SECTION_PREFIX.createKey(x, z, ySection, dimensionInfo));
            if (bytes != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void writeWorldData(WorldData worldData) {
        var allayWorldData = (AllayWorldData) worldData;
        var levelDat = path.resolve(FILE_LEVEL_DAT).toFile();
        try (var output = new FileOutputStream(levelDat);
             var byteArrayOutputStream = new ByteArrayOutputStream();
             var nbtOutputStream = NbtUtils.createWriterLE(byteArrayOutputStream)) {
            if (levelDat.exists()) {
                Files.copy(path.resolve(FILE_LEVEL_DAT), path.resolve(FILE_LEVEL_DAT_OLD), StandardCopyOption.REPLACE_EXISTING);
            }

            // 1.Current version
            output.write(LevelDBUtils.int2ByteArrayLE(WorldDataCodec.CURRENT_STORAGE_VERSION));

            nbtOutputStream.writeTag(WorldDataCodec.writeWorldDataToNBT(allayWorldData));
            var data = byteArrayOutputStream.toByteArray();

            // 2.Data length
            output.write(LevelDBUtils.int2ByteArrayLE(data.length));

            // 3.Data
            output.write(data);
        } catch (IOException e) {
            throw new WorldStorageException(e);
        }
    }

    @Override
    public WorldData readWorldData() {
        var levelDat = path.resolve(FILE_LEVEL_DAT).toFile();
        if (!levelDat.exists()) {
            return createWorldData(worldName);
        }

        try (var input = new FileInputStream(levelDat)) {
            // current_version + data length
            // noinspection ResultOfMethodCallIgnored
            input.skip(8);
            try (NBTInputStream readerLE = NbtUtils.createReaderLE(new ByteArrayInputStream(input.readAllBytes()))) {
                NbtMap nbt = (NbtMap) readerLE.readTag();
                return WorldDataCodec.readWorldDataFromNBT(nbt);
            }
        } catch (IOException e) {
            throw new WorldStorageException(e);
        }
    }

    @Override
    public String getName() {
        return "ROCKSDB";
    }

    private WorldData createWorldData(String worldName) {
        var levelDat = path.resolve(FILE_LEVEL_DAT).toFile();
        try {
            // noinspection ResultOfMethodCallIgnored
            levelDat.createNewFile();
            var worldData = AllayWorldData
                    .builder()
                    .displayName(worldName)
                    .build();
            writeWorldData(worldData);
            Files.copy(levelDat.toPath(), path.resolve(FILE_LEVEL_DAT_OLD), StandardCopyOption.REPLACE_EXISTING);
            return worldData;
        } catch (IOException e) {
            throw new WorldStorageException(e);
        }
    }

    @Override
    public void shutdown() {
        db.close();
        options.close();
    }
}
