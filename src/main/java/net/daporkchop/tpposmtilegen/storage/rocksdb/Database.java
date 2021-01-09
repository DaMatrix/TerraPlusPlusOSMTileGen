/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2021 DaPorkchop_
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

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import net.daporkchop.lib.common.function.throwing.EBiConsumer;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public final class Database implements AutoCloseable {
    public static final DBOptions DB_OPTIONS;
    public static final ColumnFamilyOptions COLUMN_OPTIONS;
    public static final ReadOptions READ_OPTIONS;
    public static final WriteOptions WRITE_OPTIONS;
    public static final WriteOptions SYNC_WRITE_OPTIONS;

    static {
        RocksDB.loadLibrary(); //ensure rocksdb native library is loaded before creating options instances

        DB_OPTIONS = new DBOptions()
                .setEnv(Env.getDefault().setBackgroundThreads(CPU_COUNT))
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setSkipStatsUpdateOnDbOpen(true)
                .setCompactionReadaheadSize(16L << 20L)
                .setAllowConcurrentMemtableWrite(true)
                .setIncreaseParallelism(CPU_COUNT)
                .setMaxSubcompactions(CPU_COUNT)
                .setKeepLogFileNum(2L)
                .setMaxOpenFiles(1024)
                .setAllowMmapReads(true)
                .setAllowMmapWrites(true);

        COLUMN_OPTIONS = new ColumnFamilyOptions()
                .setArenaBlockSize(1L << 20)
                .setOptimizeFiltersForHits(true)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionOptionsUniversal(new CompactionOptionsUniversal()
                        .setAllowTrivialMove(true));

        READ_OPTIONS = new ReadOptions();
        WRITE_OPTIONS = new WriteOptions();
        SYNC_WRITE_OPTIONS = new WriteOptions(WRITE_OPTIONS).setSync(true);
    }

    @Getter
    private final RocksDB delegate;
    private final List<ColumnFamilyHandle> columns;
    private final CloseableThreadLocal<WriteBatch> batches;

    private Database(@NonNull RocksDB delegate, @NonNull List<ColumnFamilyHandle> columns, boolean autoFlush) {
        this.delegate = delegate;
        this.columns = columns;
        this.batches = new CloseableThreadLocal<WriteBatch>() {
            @Override
            protected WriteBatch initialValue0() throws Exception {
                return autoFlush ? new AutoFlushingWriteBatch(delegate, 64L << 20L) : new WriteBatch(delegate);
            }
        };
    }

    public WriteBatch batch() {
        return this.batches.get();
    }

    public WriteBatch newNotAutoFlushingWriteBatch() {
        return new WriteBatch(this.delegate);
    }

    public void flush() throws Exception {
        this.batches.forEach(WriteBatch::flush);
    }

    @Override
    public void close() throws Exception {
        this.batches.close();
        this.columns.forEach(ColumnFamilyHandle::close);
        this.delegate.close();
    }

    @Setter
    public static final class Builder {
        private final List<ColumnFamilyDescriptor> columns = new ArrayList<>();
        private final List<EBiConsumer<Database, ColumnFamilyHandle>> factories = new ArrayList<>();
        private boolean autoFlush;

        public Builder() {
            this.columns.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, COLUMN_OPTIONS));
            this.factories.add(null);
        }

        public Builder add(@NonNull String name, @NonNull EBiConsumer<Database, ColumnFamilyHandle> factory) {
            return this.add(name, COLUMN_OPTIONS, factory);
        }

        public Builder add(@NonNull String name, @NonNull CompressionType compression, @NonNull EBiConsumer<Database, ColumnFamilyHandle> factory) {
            ColumnFamilyOptions options;
            if (compression == COLUMN_OPTIONS.compressionType()) {
                options = COLUMN_OPTIONS;
            } else {
                options = new ColumnFamilyOptions(COLUMN_OPTIONS).setCompressionType(compression);
            }
            return this.add(name, options, factory);
        }

        public Builder add(@NonNull String name, @NonNull ColumnFamilyOptions descriptor, @NonNull EBiConsumer<Database, ColumnFamilyHandle> factory) {
            this.columns.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), descriptor));
            this.factories.add(factory);
            return this;
        }

        public Database build(@NonNull Path path) throws Exception {
            List<ColumnFamilyHandle> columns = new ArrayList<>(this.columns.size());
            RocksDB db = RocksDB.open(DB_OPTIONS, path.toString(), this.columns, columns);
            Database database = new Database(db, columns, this.autoFlush);

            for (int i = 1; i < columns.size(); i++) {
                this.factories.get(i).acceptThrowing(database, columns.get(i));
            }

            return database;
        }
    }
}
