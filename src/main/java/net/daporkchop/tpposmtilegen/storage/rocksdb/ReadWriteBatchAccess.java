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
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class ReadWriteBatchAccess implements DBAccess {
    @NonNull
    protected final DBReadAccess readDelegate;
    @NonNull
    protected final DBWriteAccess writeDelegate;

    @Override
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        return this.readDelegate.get(columnFamilyHandle, key);
    }

    @Override
    public List<@NonNull byte[]> multiGetAsList(@NonNull List<@NonNull ColumnFamilyHandle> columnFamilyHandleList, @NonNull List<@NonNull byte[]> keys) throws Exception {
        return this.readDelegate.multiGetAsList(columnFamilyHandleList, keys);
    }

    @Override
    public DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle) throws Exception {
        return this.readDelegate.iterator(columnFamilyHandle);
    }

    @Override
    public DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] fromInclusive, @NonNull byte[] toExclusive) throws Exception {
        return this.readDelegate.iterator(columnFamilyHandle, fromInclusive, toExclusive);
    }

    @Override
    public boolean isDirectRead() {
        return true;
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.writeDelegate.put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.writeDelegate.put(columnFamilyHandle, key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.writeDelegate.merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.writeDelegate.delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        this.writeDelegate.deleteRange(columnFamilyHandle, beginKey, endKey);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.writeDelegate.getDataSize();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.writeDelegate.isDirty();
    }

    @Override
    public void flush() throws Exception {
        this.writeDelegate.flush();
    }

    @Override
    public void clear() throws Exception {
        this.writeDelegate.clear();
    }

    @Override
    public void close() throws Exception {
        //no-op
    }

    @Override
    public boolean threadSafe() {
        return true;
    }
}
