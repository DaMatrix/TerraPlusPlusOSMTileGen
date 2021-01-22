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
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.util.Threading;
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

    public void markDirty(@NonNull DBAccess access, long id) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            access.put(this.column, key, EMPTY_BYTE_ARRAY);
        } finally {
            recycler.release(key);
        }
    }

    public void markDirty(@NonNull DBAccess access, @NonNull LongList ids) throws Exception {
        if (ids.isEmpty()) { //nothing to do
            return;
        }

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            for (int i = 0, size = ids.size(); i < size; i++) {
                long id = ids.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                access.put(this.column, key, EMPTY_BYTE_ARRAY);
            }
        } finally {
            recycler.release(key);
        }
    }

    public boolean get(@NonNull DBAccess access, long id) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            return access.get(this.column, key) != null;
        } finally {
            recycler.release(key);
        }
    }

    public void unmarkDirty(@NonNull DBAccess access, long id) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            access.delete(this.column, key);
        } finally {
            recycler.release(key);
        }
    }

    public void forEachParallel(@NonNull DBAccess access, @NonNull LongConsumer callback) throws Exception {
        Threading.iterateParallel(1024,
                c -> {
                    try (RocksIterator itr = access.iterator(this.column)) {
                        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                            c.accept(itr.key());
                        }
                    }
                },
                k -> {
                    long pos = PUnsafe.getLong(k, PUnsafe.ARRAY_BYTE_BASE_OFFSET);
                    if (PlatformInfo.IS_LITTLE_ENDIAN) {
                        pos = Long.reverseBytes(pos);
                    }
                    callback.accept(pos);
                });
    }

    public long count(@NonNull DBAccess access) throws Exception {
        long count = 0L;
        try (RocksIterator iterator = access.iterator(this.column)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        }
        return count;
    }
}
