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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class ThreadLocalDBAccess implements DBAccess {
    @NonNull
    protected final CloseableThreadLocal<DBAccess> delegate;

    @Override
    public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        return this.delegate.get().get(columnFamilyHandle, key);
    }

    @Override
    public List<byte[]> multiGetAsList(List<ColumnFamilyHandle> columnFamilyHandleList, List<byte[]> keys) throws Exception {
        return this.delegate.get().multiGetAsList(columnFamilyHandleList, keys);
    }

    @Override
    public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle) throws Exception {
        return this.delegate.get().iterator(columnFamilyHandle);
    }

    @Override
    public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle, ReadOptions options) throws Exception {
        return this.delegate.get().iterator(columnFamilyHandle, options);
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        this.delegate.get().put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws Exception {
        this.delegate.get().put(columnFamilyHandle, key, value);
    }

    @Override
    public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        this.delegate.get().merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        this.delegate.get().delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) throws Exception {
        this.delegate.get().deleteRange(columnFamilyHandle, beginKey, endKey);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.delegate.get().getDataSize();
    }

    @Override
    public void flush(boolean sync) throws Exception {
        this.delegate.forEach((EConsumer<DBAccess>) a -> a.flush(sync));
    }

    @Override
    public void clear() throws Exception {
        this.delegate.forEach((EConsumer<DBAccess>) DBAccess::clear);
    }

    @Override
    public void close() throws Exception {
        this.delegate.close();
    }
}
