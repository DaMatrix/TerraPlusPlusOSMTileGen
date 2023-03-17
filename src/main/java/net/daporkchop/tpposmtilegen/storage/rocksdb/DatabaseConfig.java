/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * Any persons and/or organizations using this software must include the above copyright notice and this permission notice,
 * provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.tpposmtilegen.storage.rocksdb;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.With;
import net.daporkchop.lib.common.util.PValidation;
import org.rocksdb.AccessHint;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ClockCache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.EnvOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Priority;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TxnDBWritePolicy;
import org.rocksdb.WriteOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
@SuppressWarnings({ "ExplicitArgumentCanBeLambda", "resource" })
@Builder(toBuilder = true)
@Getter
@With
public final class DatabaseConfig {
    public static final DatabaseConfig RW_GENERAL;
    public static final DatabaseConfig RW_BULK_LOAD;
    public static final DatabaseConfig RW_LITE;
    public static final DatabaseConfig RW_LITE_BULK_LOAD;

    public static final DatabaseConfig RO_GENERAL;
    public static final DatabaseConfig RO_LITE;

    private static final long TABLE_SIZE_BASE_KIB = 65536L;

    static {
        RocksDB.loadLibrary(); //ensure rocksdb native library is loaded before creating options instances

        @Getter
        class ColumnFamilySettings {
            private final long dataBlockSizeBytes;
            private final int cacheShardBits;

            private final Cache blockCache;

            public ColumnFamilySettings(long dataBlockSizeKib, int cacheShardBits) {
                this.dataBlockSizeBytes = dataBlockSizeKib << 10L;
                this.cacheShardBits = cacheShardBits;

                this.blockCache = new ClockCache((dataBlockSizeKib << 11L) * (1L << (long) cacheShardBits), cacheShardBits);
            }
        }

        EnumMap<ColumnFamilyType, ColumnFamilySettings> dataBlockSizes = new EnumMap<>(ColumnFamilyType.class);
        dataBlockSizes.put(ColumnFamilyType.FAST, new ColumnFamilySettings(4L, 15));
        dataBlockSizes.put(ColumnFamilyType.COMPACT, new ColumnFamilySettings(1024L, 7));

        RW_GENERAL = builder()
                .readOnly(false)
                .transactional(true)
                .dbOptions(new DBOptions()
                                .setEnv(Env.getDefault()
                                        .setBackgroundThreads(CPU_COUNT, Priority.HIGH)
                                        .setBackgroundThreads(CPU_COUNT, Priority.LOW))
                                .setIncreaseParallelism(CPU_COUNT)
                                .setMaxBackgroundJobs(CPU_COUNT)
                                .setMaxSubcompactions(CPU_COUNT)
                                .setCreateIfMissing(true)
                                .setCreateMissingColumnFamilies(true)
                                .setSkipStatsUpdateOnDbOpen(true)
                                .setCompactionReadaheadSize(TABLE_SIZE_BASE_KIB << 10L)
                                .setAccessHintOnCompactionStart(AccessHint.WILLNEED)
                                .setAllowConcurrentMemtableWrite(true)
                                .setKeepLogFileNum(16L)
                                .setMaxTotalWalSize(TABLE_SIZE_BASE_KIB << 10L << 2L)
                                .setAllowMmapReads(true)
                                .setAllowMmapWrites(true)
                                .setAdviseRandomOnOpen(true)
                                .setEnablePipelinedWrite(true)
                                .setMaxOpenFiles(Integer.getInteger("maxOpenFiles", -1))
                        //.setWriteBufferManager(new WriteBufferManager(1L << 30L, new LRUCache(1L << 30L)))
                )
                .transactionDBOptions(new TransactionDBOptions()
                        .setWritePolicy(TxnDBWritePolicy.WRITE_UNPREPARED)
                )
                .columnFamilyOptions(ColumnFamilyType.FAST, new ColumnFamilyOptions()
                        .setMaxWriteBufferNumber(CPU_COUNT)
                        .setTargetFileSizeBase(TABLE_SIZE_BASE_KIB << 10L)
                        .setCompactionStyle(CompactionStyle.LEVEL)
                        //we don't use compression for level 0, as training the zstd dictionary for it takes so long that it causes rocksdb to eat up my whole 96GiB of RAM
                        .setCompressionPerLevel(Arrays.asList(CompressionType.NO_COMPRESSION, CompressionType.ZSTD_COMPRESSION))
                        .setCompressionOptions(new CompressionOptions()
                                .setEnabled(true)
                                .setMaxDictBytes(64 << 10)
                                .setZStdMaxTrainBytes(64 << 20)
                                .setLevel(0))
                        .setCompactionOptionsUniversal(new CompactionOptionsUniversal()
                                .setAllowTrivialMove(true))
                        .setOptimizeFiltersForHits(true)
                        .setTableFormatConfig(new BlockBasedTableConfig()
                                .setBlockSize(dataBlockSizes.get(ColumnFamilyType.FAST).dataBlockSizeBytes)
                                .setBlockCache(dataBlockSizes.get(ColumnFamilyType.FAST).blockCache))
                        //a manual compaction run should pick all files in L0 in a single compaction run
                        .setMaxCompactionBytes(1L << 60L)
                )
                .columnFamilyOptionsBasedOn(ColumnFamilyType.FAST_SPARSE, ColumnFamilyType.FAST, baseOptions -> baseOptions)
                .columnFamilyOptionsBasedOn(ColumnFamilyType.COMPACT, ColumnFamilyType.FAST, baseOptions -> new ColumnFamilyOptions(baseOptions)
                        .setCompressionPerLevel(Collections.singletonList(CompressionType.ZSTD_COMPRESSION))
                        .setCompressionOptions(new CompressionOptions()
                                .setEnabled(true)
                                .setMaxDictBytes(64 << 10)
                                .setZStdMaxTrainBytes(64 << 20)
                                .setLevel(7))
                        .setTableFormatConfig(new BlockBasedTableConfig()
                                .setBlockSize(dataBlockSizes.get(ColumnFamilyType.COMPACT).dataBlockSizeBytes)
                                .setBlockCache(dataBlockSizes.get(ColumnFamilyType.COMPACT).blockCache))
                )
                .readOptions(ReadType.GENERAL, new ReadOptions()
                )
                .readOptionsBasedOn(ReadType.BULK_ITERATE, ReadType.GENERAL, baseOptions -> new ReadOptions(baseOptions)
                        .setFillCache(false)
                        .setReadaheadSize(TABLE_SIZE_BASE_KIB << 10L)
                )
                .writeOptions(WriteType.GENERAL, new WriteOptions()
                )
                .writeOptionsBasedOn(WriteType.SYNC, WriteType.GENERAL, baseOptions -> new WriteOptions(baseOptions)
                        .setSync(true)
                )
                .writeOptionsBasedOn(WriteType.NO_WAL, WriteType.GENERAL, baseOptions -> new WriteOptions(baseOptions)
                        .setDisableWAL(true)
                )
                .flushOptions(FlushType.GENERAL, new FlushOptions()
                        .setWaitForFlush(true)
                        .setAllowWriteStall(true)
                )
                .ingestOptions(IngestType.COPY, new IngestExternalFileOptions()
                        .setMoveFiles(false)
                        .setWriteGlobalSeqno(false)
                )
                .ingestOptionsBasedOn(IngestType.MOVE, IngestType.COPY, baseOptions -> new IngestExternalFileOptions(baseOptions.moveFiles(), baseOptions.snapshotConsistency(), baseOptions.allowGlobalSeqNo(), baseOptions.allowBlockingFlush())
                        .setIngestBehind(baseOptions.ingestBehind())
                        .setWriteGlobalSeqno(baseOptions.writeGlobalSeqno())
                        .setMoveFiles(true)
                )
                .compactRangeOptions(new CompactRangeOptions()
                        .setChangeLevel(true)
                )
                .build();

        RW_BULK_LOAD = RW_GENERAL.toBuilder()
                .transactional(false)
                .columnFamilyOptions(ColumnFamilyType.FAST, new ColumnFamilyOptions(RW_GENERAL.columnFamilyOptions(ColumnFamilyType.FAST))
                        .setDisableAutoCompactions(true)
                        //never slowdown ingest
                        .setLevel0FileNumCompactionTrigger(1 << 30)
                        .setLevel0SlowdownWritesTrigger(1 << 30)
                        .setLevel0StopWritesTrigger(1 << 30)
                        .setSoftPendingCompactionBytesLimit(0L)
                        .setHardPendingCompactionBytesLimit(0L)
                )
                .columnFamilyOptions(ColumnFamilyType.FAST_SPARSE, new ColumnFamilyOptions(RW_GENERAL.columnFamilyOptions(ColumnFamilyType.FAST_SPARSE))
                        .setDisableAutoCompactions(true)
                        //never slowdown ingest
                        .setLevel0FileNumCompactionTrigger(1 << 30)
                        .setLevel0SlowdownWritesTrigger(1 << 30)
                        .setLevel0StopWritesTrigger(1 << 30)
                        .setSoftPendingCompactionBytesLimit(0L)
                        .setHardPendingCompactionBytesLimit(0L)
                )
                .columnFamilyOptions(ColumnFamilyType.COMPACT, new ColumnFamilyOptions(RW_GENERAL.columnFamilyOptions(ColumnFamilyType.COMPACT))
                        .setDisableAutoCompactions(true)
                        //never slowdown ingest
                        .setLevel0FileNumCompactionTrigger(1 << 30)
                        .setLevel0SlowdownWritesTrigger(1 << 30)
                        .setLevel0StopWritesTrigger(1 << 30)
                        .setSoftPendingCompactionBytesLimit(0L)
                        .setHardPendingCompactionBytesLimit(0L)
                )
                .build();

        RW_LITE = RW_GENERAL.toBuilder()
                .dbOptions(new DBOptions(RW_GENERAL.dbOptions())
                        .setMaxOpenFiles(CPU_COUNT << 1)
                )
                .build();

        RW_LITE_BULK_LOAD = RW_BULK_LOAD.toBuilder()
                .dbOptions(RW_LITE.dbOptions()) //inherit RW_LITE's lowered open file limit
                .build();

        RO_GENERAL = RW_GENERAL.withReadOnly(true);
        RO_LITE = RW_LITE.withReadOnly(true);
    }

    @NonNull
    private final DBOptions dbOptions;
    @NonNull
    private final TransactionDBOptions transactionDBOptions;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Builder.ObtainVia(method = "columnFamilyOptionsByType")
    private final EnumMap<ColumnFamilyType, ColumnFamilyOptions> columnFamilyOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Builder.ObtainVia(method = "readOptionsByType")
    private final EnumMap<ReadType, ReadOptions> readOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Builder.ObtainVia(method = "writeOptionsByType")
    private final EnumMap<WriteType, WriteOptions> writeOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Builder.ObtainVia(method = "flushOptionsByType")
    private final EnumMap<FlushType, FlushOptions> flushOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Builder.ObtainVia(method = "ingestOptionsByType")
    private final EnumMap<IngestType, IngestExternalFileOptions> ingestOptionsByType;

    @NonNull
    private final CompactRangeOptions compactRangeOptions;

    private final boolean transactional;
    private final boolean readOnly;

    @Getter(lazy = true)
    private final EnvOptions envOptions = new EnvOptions(this.dbOptions);

    public EnumMap<ColumnFamilyType, ColumnFamilyOptions> columnFamilyOptionsByType() {
        return new EnumMap<>(this.columnFamilyOptionsByType);
    }

    public EnumMap<ReadType, ReadOptions> readOptionsByType() {
        return new EnumMap<>(this.readOptionsByType);
    }

    public EnumMap<WriteType, WriteOptions> writeOptionsByType() {
        return new EnumMap<>(this.writeOptionsByType);
    }

    public EnumMap<FlushType, FlushOptions> flushOptionsByType() {
        return new EnumMap<>(this.flushOptionsByType);
    }

    public EnumMap<IngestType, IngestExternalFileOptions> ingestOptionsByType() {
        return new EnumMap<>(this.ingestOptionsByType);
    }

    public ColumnFamilyOptions columnFamilyOptions(@NonNull DatabaseConfig.ColumnFamilyType type) {
        return Objects.requireNonNull(this.columnFamilyOptionsByType.get(type), type.name());
    }

    public ReadOptions readOptions(@NonNull ReadType type) {
        return Objects.requireNonNull(this.readOptionsByType.get(type), type.name());
    }

    public WriteOptions writeOptions(@NonNull WriteType type) {
        return Objects.requireNonNull(this.writeOptionsByType.get(type), type.name());
    }

    public FlushOptions flushOptions(@NonNull FlushType type) {
        return Objects.requireNonNull(this.flushOptionsByType.get(type), type.name());
    }

    public IngestExternalFileOptions ingestOptions(@NonNull IngestType type) {
        return Objects.requireNonNull(this.ingestOptionsByType.get(type), type.name());
    }

    /**
     * @author DaPorkchop_
     */
    public enum ColumnFamilyType {
        FAST,
        FAST_SPARSE,
        COMPACT,
    }

    /**
     * @author DaPorkchop_
     */
    public enum ReadType {
        GENERAL,
        BULK_ITERATE,
    }

    /**
     * @author DaPorkchop_
     */
    public enum WriteType {
        GENERAL,
        SYNC,
        NO_WAL,
    }

    /**
     * @author DaPorkchop_
     */
    public enum FlushType {
        GENERAL,
    }

    /**
     * @author DaPorkchop_
     */
    public enum IngestType {
        COPY,
        MOVE,
    }

    public static class DatabaseConfigBuilder {
        public DatabaseConfigBuilder columnFamilyOptions(@NonNull DatabaseConfig.ColumnFamilyType key, @NonNull ColumnFamilyOptions value) {
            //noinspection ConstantValue
            if (this.columnFamilyOptionsByType == null) {
                this.columnFamilyOptionsByType(new EnumMap<>(ColumnFamilyType.class));
            }
            this.columnFamilyOptionsByType.put(key, value);
            return this;
        }

        public DatabaseConfigBuilder readOptions(@NonNull ReadType key, @NonNull ReadOptions value) {
            //noinspection ConstantValue
            if (this.readOptionsByType == null) {
                this.readOptionsByType(new EnumMap<>(ReadType.class));
            }
            this.readOptionsByType.put(key, value);
            return this;
        }

        public DatabaseConfigBuilder writeOptions(@NonNull WriteType key, @NonNull WriteOptions value) {
            //noinspection ConstantValue
            if (this.writeOptionsByType == null) {
                this.writeOptionsByType(new EnumMap<>(WriteType.class));
            }
            this.writeOptionsByType.put(key, value);
            return this;
        }

        public DatabaseConfigBuilder flushOptions(@NonNull FlushType key, @NonNull FlushOptions value) {
            //noinspection ConstantValue
            if (this.flushOptionsByType == null) {
                this.flushOptionsByType(new EnumMap<>(FlushType.class));
            }
            this.flushOptionsByType.put(key, value);
            return this;
        }

        public DatabaseConfigBuilder ingestOptions(@NonNull IngestType key, @NonNull IngestExternalFileOptions value) {
            //noinspection ConstantValue
            if (this.ingestOptionsByType == null) {
                this.ingestOptionsByType(new EnumMap<>(IngestType.class));
            }
            this.ingestOptionsByType.put(key, value);
            return this;
        }

        public DatabaseConfigBuilder columnFamilyOptionsBasedOn(@NonNull DatabaseConfig.ColumnFamilyType key, @NonNull DatabaseConfig.ColumnFamilyType base, @NonNull UnaryOperator<ColumnFamilyOptions> mapper) {
            PValidation.checkArg(this.columnFamilyOptionsByType.containsKey(base), "base %s '%s' isn't present", ColumnFamilyType.class.getTypeName(), base);
            return this.columnFamilyOptions(key, mapper.apply(this.columnFamilyOptionsByType.get(base)));
        }

        public DatabaseConfigBuilder readOptionsBasedOn(@NonNull ReadType key, @NonNull ReadType base, @NonNull UnaryOperator<ReadOptions> mapper) {
            PValidation.checkArg(this.readOptionsByType.containsKey(base), "base %s '%s' isn't present", ReadType.class.getTypeName(), base);
            return this.readOptions(key, mapper.apply(this.readOptionsByType.get(base)));
        }

        public DatabaseConfigBuilder writeOptionsBasedOn(@NonNull WriteType key, @NonNull WriteType base, @NonNull UnaryOperator<WriteOptions> mapper) {
            PValidation.checkArg(this.writeOptionsByType.containsKey(base), "base %s '%s' isn't present", WriteType.class.getTypeName(), base);
            return this.writeOptions(key, mapper.apply(this.writeOptionsByType.get(base)));
        }

        public DatabaseConfigBuilder flushOptionsBasedOn(@NonNull FlushType key, @NonNull FlushType base, @NonNull UnaryOperator<FlushOptions> mapper) {
            PValidation.checkArg(this.flushOptionsByType.containsKey(base), "base %s '%s' isn't present", FlushType.class.getTypeName(), base);
            return this.flushOptions(key, mapper.apply(this.flushOptionsByType.get(base)));
        }

        public DatabaseConfigBuilder ingestOptionsBasedOn(@NonNull IngestType key, @NonNull IngestType base, @NonNull UnaryOperator<IngestExternalFileOptions> mapper) {
            PValidation.checkArg(this.ingestOptionsByType.containsKey(base), "base %s '%s' isn't present", IngestType.class.getTypeName(), base);
            return this.ingestOptions(key, mapper.apply(this.ingestOptionsByType.get(base)));
        }
    }
}
