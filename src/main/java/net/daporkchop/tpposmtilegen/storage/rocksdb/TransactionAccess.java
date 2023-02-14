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
import net.daporkchop.tpposmtilegen.util.Utils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Transaction;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class TransactionAccess implements DBAccess {
    private static byte[] toByteArray(@NonNull ByteBuffer buf) {
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        return arr;
    }

    @NonNull
    protected final DatabaseConfig config;
    @NonNull
    protected final OptimisticTransactionDB db;
    @NonNull
    protected final Transaction transaction;

    @Override
    public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        return this.transaction.get(columnFamilyHandle, this.config.readOptions(DatabaseConfig.ReadType.GENERAL), key);
    }

    @Override
    public List<byte[]> multiGetAsList(List<ColumnFamilyHandle> columnFamilyHandleList, List<byte[]> keys) throws Exception {
        return this.transaction.multiGetAsList(this.config.readOptions(DatabaseConfig.ReadType.GENERAL), columnFamilyHandleList, keys);
    }

    @Override
    public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle) throws Exception {
        return this.transaction.getIterator(this.config.readOptions(DatabaseConfig.ReadType.BULK_ITERATE), columnFamilyHandle);
    }

    @Override
    public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle, ReadOptions options) throws Exception {
        return this.transaction.getIterator(options, columnFamilyHandle);
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        this.transaction.put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws Exception {
        this.put(columnFamilyHandle, toByteArray(key), toByteArray(value));
    }

    @Override
    public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        this.transaction.merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        this.transaction.delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) throws Exception {
        try (Slice toSlice = new Slice(endKey);
             ReadOptions options = new ReadOptions(this.config.readOptions(DatabaseConfig.ReadType.GENERAL)).setIterateUpperBound(toSlice);
             RocksIterator iterator = this.transaction.getIterator(options, columnFamilyHandle)) {
            for (iterator.seek(beginKey); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();

                if (Utils.BYTES_COMPARATOR.compare(endKey, key) >= 0) {
                    //iterating over a transaction can go far beyond the actual iteration bound (rocksdb bug), so we have to manually check
                    return;
                }

                this.transaction.delete(columnFamilyHandle, key);
            }
        }
    }

    @Override
    public long getDataSize() throws Exception {
        return this.transaction.getWriteBatch().getWriteBatch().getDataSize();
    }

    @Override
    public void flush() throws Exception {
        this.transaction.commit();
    }

    @Override
    public void clear() throws Exception {
        this.transaction.rollback();
    }

    @Override
    public void close() throws Exception {
        this.clear();

        this.transaction.close();
    }

    @Override
    public boolean threadSafe() {
        return false;
    }
}
