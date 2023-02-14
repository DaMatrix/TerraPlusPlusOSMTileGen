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
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
class FlushableWriteBatch implements DBAccess {
    @NonNull
    protected final DatabaseConfig config;
    @NonNull
    protected final RocksDB db;
    protected final WriteBatch batch = new WriteBatch();
    protected boolean dirty = false;

    @Override
    public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> multiGetAsList(List<ColumnFamilyHandle> columnFamilyHandleList, List<byte[]> keys) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle, ReadOptions options) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        this.batch.put(columnFamilyHandle, key, value);
        this.dirty = true;
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws Exception {
        this.batch.put(columnFamilyHandle, key, value);
        this.dirty = true;
    }

    @Override
    public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        this.batch.merge(columnFamilyHandle, key, value);
        this.dirty = true;
    }

    @Override
    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        this.batch.delete(columnFamilyHandle, key);
        this.dirty = true;
    }

    @Override
    public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) throws Exception {
        this.batch.deleteRange(columnFamilyHandle, beginKey, endKey);
        this.dirty = true;
    }

    @Override
    public long getDataSize() throws Exception {
        return this.batch.getDataSize();
    }

    @Override
    public void flush() throws Exception {
        if (this.dirty) {
            try {
                this.db.write(this.config.writeOptions(DatabaseConfig.WriteType.NO_WAL), this.batch);
            } finally {
                this.batch.clear();
                this.dirty = false;
            }
        }
    }

    @Override
    public void clear() throws Exception {
        if (this.dirty) {
            this.batch.clear();
            this.dirty = false;
        }
    }

    @Override
    public void close() throws Exception {
        this.flush();

        this.batch.close();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }
}
