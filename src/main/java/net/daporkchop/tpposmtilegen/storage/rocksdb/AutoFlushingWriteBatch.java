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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * Wrapper around a RocksDB {@link org.rocksdb.WriteBatch} with the ability to auto-flush.
 *
 * @author DaPorkchop_
 */
class AutoFlushingWriteBatch extends FlushableWriteBatch {
    protected final long threshold;

    public AutoFlushingWriteBatch(RocksDB db, long threshold) {
        super(db);

        this.threshold = positive(threshold, "threshold");
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        super.put(columnFamilyHandle, key, value);
        this.checkFlush();
    }

    @Override
    public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws Exception {
        super.put(columnFamilyHandle, key, value);
        this.checkFlush();
    }

    @Override
    public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception {
        super.merge(columnFamilyHandle, key, value);
        this.checkFlush();
    }

    @Override
    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception {
        super.delete(columnFamilyHandle, key);
        this.checkFlush();
    }

    @Override
    public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) throws Exception {
        super.deleteRange(columnFamilyHandle, beginKey, endKey);
        this.checkFlush();
    }

    protected void checkFlush() throws Exception {
        if (this.batch.getDataSize() >= this.threshold) {
            this.flush(false);
        }
    }
}
