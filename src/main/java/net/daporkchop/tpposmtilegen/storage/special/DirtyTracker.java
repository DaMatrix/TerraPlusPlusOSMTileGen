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

package net.daporkchop.tpposmtilegen.storage.special;

import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WriteBatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

import java.util.function.LongConsumer;

/**
 * @author DaPorkchop_
 */
public final class DirtyTracker extends WrappedRocksDB {
    public DirtyTracker(Database database, ColumnFamilyHandle column) {
        super(database, column);
    }

    @Override
    protected int keySize() {
        return 8;
    }

    public void markDirty(@NonNull WriteBatch batch, long id) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            batch.put(this.column, key, EMPTY_BYTE_ARRAY);
        } finally {
            recycler.release(key);
        }
    }

    public void markDirty(@NonNull WriteBatch batch, @NonNull LongList ids) throws Exception {
        if (ids.isEmpty()) { //nothing to do
            return;
        }

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            for (int i = 0, size = ids.size(); i < size; i++) {
                long id = ids.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                batch.put(this.column, key, EMPTY_BYTE_ARRAY);
            }
        } finally {
            recycler.release(key);
        }
    }

    public void forEach(@NonNull LongConsumer callback) throws Exception {
        try (RocksIterator iterator = this.database.delegate().newIterator(this.column)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                long id = PUnsafe.getLong(iterator.key(), PUnsafe.ARRAY_BYTE_BASE_OFFSET);
                callback.accept(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            }
        }
    }
}
