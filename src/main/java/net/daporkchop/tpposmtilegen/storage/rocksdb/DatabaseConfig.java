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
import lombok.Singular;
import lombok.With;
import net.daporkchop.lib.common.util.PValidation;
import org.rocksdb.AccessHint;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Priority;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TxnDBWritePolicy;
import org.rocksdb.WriteOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
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
    public static final DatabaseConfig RW_LITE;

    public static final DatabaseConfig RO_GENERAL;
    public static final DatabaseConfig RO_LITE;

    private static final long tableSizeBase = 65536L;
    private static final long dataBlockSize = 1024L;
    private static final int cacheShardBits = 7;

    static {
        RocksDB.loadLibrary(); //ensure rocksdb native library is loaded before creating options instances

        RW_GENERAL = builder()
                .readOnly(false)
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
                        .setCompactionReadaheadSize(tableSizeBase << 10L)
                        .setAccessHintOnCompactionStart(AccessHint.WILLNEED)
                        .setAllowFAllocate(true)
                        .setAllowConcurrentMemtableWrite(true)
                        .setKeepLogFileNum(16L)
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
                .columnFamilyOptions(ColumnType.FAST, new ColumnFamilyOptions()
                        .setMaxWriteBufferNumber(CPU_COUNT)
                        .setTargetFileSizeBase(tableSizeBase << 10L)
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
                )
                .columnFamilyOptionsBasedOn(ColumnType.COMPACT, ColumnType.FAST, baseOptions -> new ColumnFamilyOptions(baseOptions)
                        .setCompressionPerLevel(Collections.singletonList(CompressionType.ZSTD_COMPRESSION))
                        .setCompressionOptions(new CompressionOptions()
                                .setEnabled(true)
                                .setMaxDictBytes(64 << 10)
                                .setZStdMaxTrainBytes(64 << 20)
                                .setLevel(7))
                        .setTableFormatConfig(new BlockBasedTableConfig()
                                .setBlockSize(dataBlockSize << 10L)
                                .setBlockCache(new LRUCache((dataBlockSize << 11L) * (1L << (long) cacheShardBits), cacheShardBits)))
                )
                .readOptions(ReadType.GENERAL, new ReadOptions()
                )
                .readOptionsBasedOn(ReadType.BULK_ITERATE, ReadType.GENERAL, baseOptions -> new ReadOptions(baseOptions)
                        .setFillCache(false)
                        .setReadaheadSize(tableSizeBase << 10L)
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
                .build();

        RW_LITE = RW_GENERAL.toBuilder()
                .dbOptions(new DBOptions(RW_GENERAL.dbOptions())
                        .setMaxOpenFiles(CPU_COUNT << 1)
                )
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
    @Singular("columnFamilyOptions")
    private final Map<ColumnType, ColumnFamilyOptions> columnFamilyOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Singular("readOptions")
    private final Map<ReadType, ReadOptions> readOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Singular("writeOptions")
    private final Map<WriteType, WriteOptions> writeOptionsByType;

    @NonNull
    @Getter(AccessLevel.NONE)
    @With(AccessLevel.NONE)
    @Singular("flushOptions")
    private final Map<FlushType, FlushOptions> flushOptionsByType;

    private final boolean readOnly;

    public ColumnFamilyOptions columnFamilyOptions(@NonNull ColumnType type) {
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

    /**
     * @author DaPorkchop_
     */
    public enum ColumnType {
        FAST,
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

    public static class DatabaseConfigBuilder {
        public DatabaseConfigBuilder columnFamilyOptionsBasedOn(@NonNull ColumnType key, @NonNull ColumnType base, @NonNull UnaryOperator<ColumnFamilyOptions> mapper) {
            PValidation.checkArg(this.columnFamilyOptionsByType$key.contains(base), "base %s '%s' isn't present", ColumnType.class.getTypeName(), base);
            return this.columnFamilyOptions(key, mapper.apply(this.columnFamilyOptionsByType$value.get(this.columnFamilyOptionsByType$key.lastIndexOf(base))));
        }

        public DatabaseConfigBuilder readOptionsBasedOn(@NonNull ReadType key, @NonNull ReadType base, @NonNull UnaryOperator<ReadOptions> mapper) {
            PValidation.checkArg(this.readOptionsByType$key.contains(base), "base %s '%s' isn't present", ReadType.class.getTypeName(), base);
            return this.readOptions(key, mapper.apply(this.readOptionsByType$value.get(this.readOptionsByType$key.lastIndexOf(base))));
        }

        public DatabaseConfigBuilder writeOptionsBasedOn(@NonNull WriteType key, @NonNull WriteType base, @NonNull UnaryOperator<WriteOptions> mapper) {
            PValidation.checkArg(this.writeOptionsByType$key.contains(base), "base %s '%s' isn't present", WriteType.class.getTypeName(), base);
            return this.writeOptions(key, mapper.apply(this.writeOptionsByType$value.get(this.writeOptionsByType$key.lastIndexOf(base))));
        }

        public DatabaseConfigBuilder flushOptionsBasedOn(@NonNull FlushType key, @NonNull FlushType base, @NonNull UnaryOperator<FlushOptions> mapper) {
            PValidation.checkArg(this.flushOptionsByType$key.contains(base), "base %s '%s' isn't present", FlushType.class.getTypeName(), base);
            return this.flushOptions(key, mapper.apply(this.flushOptionsByType$value.get(this.flushOptionsByType$key.lastIndexOf(base))));
        }
    }
}
