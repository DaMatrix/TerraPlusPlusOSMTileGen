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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
abstract class FlushableWriteBatch implements DBWriteAccess {
    @NonNull
    protected final DatabaseConfig config;
    @NonNull
    protected final DatabaseConfig.WriteType writeType;
    @NonNull
    protected final RocksDB db;
    protected final WriteBatch batch = new WriteBatch();

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.batch.put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.batch.put(columnFamilyHandle, key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.batch.merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.batch.delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        this.batch.deleteRange(columnFamilyHandle, beginKey, endKey);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.batch.getDataSize() - NativeRocksHelper.writeBatchHeaderSize();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.batch.count() != 0;
    }

    @Override
    public void flush() throws Exception {
        if (this.isDirty()) {
            try {
                this.db.write(this.config.writeOptions(this.writeType), this.batch);
            } finally {
                this.batch.clear();
            }
        }
    }

    @Override
    public void clear() throws Exception {
        this.batch.clear();
    }

    @Override
    public void close() throws Exception {
        try (WriteBatch batch = this.batch) {
            this.flush();
        }
    }

    @Override
    public boolean threadSafe() {
        return true;
    }

    /**
     * Wrapper around a RocksDB {@link WriteBatch}.
     *
     * @author DaPorkchop_
     */
    public static class Regular extends FlushableWriteBatch implements DBWriteAccess.Transactional {
        public Regular(DatabaseConfig config, DatabaseConfig.WriteType writeType, RocksDB db) {
            super(config, writeType, db);
        }

        @Override
        public void pushCheckpoint() throws Exception {
            this.batch.setSavePoint();
        }

        @Override
        public void popCheckpoint() throws Exception {
            this.batch.rollbackToSavePoint();
        }
    }

    /**
     * Wrapper around a RocksDB {@link WriteBatch} with the ability to auto-flush upon writing more than a given amount of data.
     *
     * @author DaPorkchop_
     */
    public static class AutoFlushing extends FlushableWriteBatch {
        protected final long threshold;

        public AutoFlushing(DatabaseConfig config, DatabaseConfig.WriteType writeType, RocksDB db, long threshold) {
            super(config, writeType, db);

            this.threshold = positive(threshold, "threshold");
        }

        @Override
        public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
            super.put(columnFamilyHandle, key, value);
            this.checkFlush();
        }

        @Override
        public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
            super.put(columnFamilyHandle, key, value);
            this.checkFlush();
        }

        @Override
        public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
            super.merge(columnFamilyHandle, key, value);
            this.checkFlush();
        }

        @Override
        public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
            super.delete(columnFamilyHandle, key);
            this.checkFlush();
        }

        @Override
        public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
            super.deleteRange(columnFamilyHandle, beginKey, endKey);
            this.checkFlush();
        }

        protected void checkFlush() throws Exception {
            if (this.batch.getDataSize() >= this.threshold) {
                this.flush();
            }
        }
    }
}
