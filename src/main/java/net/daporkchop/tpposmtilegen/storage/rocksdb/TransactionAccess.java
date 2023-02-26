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

import static net.daporkchop.lib.common.util.PValidation.*;

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
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        return this.transaction.get(columnFamilyHandle, this.config.readOptions(DatabaseConfig.ReadType.GENERAL), key);
    }

    @Override
    public List<@NonNull byte[]> multiGetAsList(@NonNull List<@NonNull ColumnFamilyHandle> columnFamilyHandleList, @NonNull List<@NonNull byte[]> keys) throws Exception {
        return this.transaction.multiGetAsList(this.config.readOptions(DatabaseConfig.ReadType.GENERAL), columnFamilyHandleList, keys);
    }

    @Override
    public DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle) throws Exception {
        return new DBIterator.SimpleRocksIteratorWrapper(this.transaction.getIterator(this.config.readOptions(DatabaseConfig.ReadType.BULK_ITERATE), columnFamilyHandle));
    }

    @Override
    public DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] fromInclusive, @NonNull byte[] toExclusive) throws Exception {
        Slice fromInclusiveSlice = null;
        Slice toExclusiveSlice = null;
        ReadOptions readOptions = null;
        try {
            fromInclusiveSlice = new Slice(fromInclusive);
            toExclusiveSlice = new Slice(toExclusive);
            readOptions = new ReadOptions(this.config.readOptions(DatabaseConfig.ReadType.GENERAL))
                    .setIterateLowerBound(fromInclusiveSlice)
                    .setIterateUpperBound(toExclusiveSlice);

            byte[] fromInclusiveClone = fromInclusive.clone();
            byte[] toExclusiveClone = toExclusive.clone();
            return new DBIterator.SimpleRangedRocksIteratorWrapper(this.transaction.getIterator(readOptions, columnFamilyHandle), readOptions, fromInclusiveSlice, toExclusiveSlice) {
                @Override
                public void seekToFirst() {
                    this.seekCeil(fromInclusiveClone);
                }

                @Override
                public void seekToLast() {
                    this.seekFloor(toExclusiveClone);

                    if (Arrays.equals(toExclusiveClone, this.key())) { //seek back by one
                        this.prev();
                    }
                }

                @Override
                public boolean isValid() {
                    //iterating over a transaction can go far beyond the actual iteration bound (rocksdb bug), so we have to manually check
                    return super.isValid()
                           && Utils.BYTES_COMPARATOR.compare(fromInclusiveClone, this.key()) <= 0
                           && Utils.BYTES_COMPARATOR.compare(toExclusiveClone, this.key()) > 0;
                }
            };
        } catch (Exception e) {
            if (readOptions != null) {
                readOptions.close();
            }
            if (toExclusiveSlice != null) {
                toExclusiveSlice.close();
            }
            if (fromInclusiveSlice != null) {
                fromInclusiveSlice.close();
            }
            throw e;
        }
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.transaction.put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.put(columnFamilyHandle, toByteArray(key), toByteArray(value));
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.transaction.merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.transaction.delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        try (DBIterator iterator = this.iterator(columnFamilyHandle, beginKey, endKey)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();

                checkState(Utils.BYTES_COMPARATOR.compare(beginKey, key) <= 0, "before beginKey");
                checkState(Utils.BYTES_COMPARATOR.compare(endKey, key) > 0, "exceeding endKey");

                this.transaction.delete(columnFamilyHandle, iterator.key());
            }
        }
    }

    @Override
    public long getDataSize() throws Exception {
        return this.transaction.getWriteBatch().getWriteBatch().getDataSize();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.transaction.getWriteBatch().getWriteBatch().count() != 0;
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
