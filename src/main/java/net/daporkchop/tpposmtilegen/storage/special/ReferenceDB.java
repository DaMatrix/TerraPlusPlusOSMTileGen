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
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WriteBatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Tracks references between elements.
 * <p>
 * struct Key {
 * long id; //the id of the element that is referenced
 * long referentId; //the id of the referring element
 * };
 *
 * @author DaPorkchop_
 */
public final class ReferenceDB extends WrappedRocksDB {
    public ReferenceDB(Database database, ColumnFamilyHandle column) {
        super(database, column);
    }

    @Override
    protected int keySize() {
        return 16;
    }

    public void addReference(@NonNull WriteBatch batch, int type, long id, int referentType, long referent) throws Exception {
        id = Element.addTypeToId(type, id);
        referent = Element.addTypeToId(referentType, referent);

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(referent) : referent);
            batch.put(this.column, key, EMPTY_BYTE_ARRAY);
        } finally {
            recycler.release(key);
        }
    }

    public void addReferences(@NonNull WriteBatch batch, int type, @NonNull LongList ids, int referentType, long referent) throws Exception {
        int size = ids.size();
        if (size == 0) {
            return;
        }

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] key = recycler.get();
        try {
            referent = Element.addTypeToId(referentType, referent);
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(referent) : referent);
            for (int i = 0; i < size; i++) {
                long id = Element.addTypeToId(type, ids.getLong(i));
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                batch.put(this.column, key, EMPTY_BYTE_ARRAY);
            }
        } finally {
            recycler.release(key);
        }
    }

    public void deleteReferencesTo(@NonNull WriteBatch batch, int type, long id) throws Exception {
        id = Element.addTypeToId(type, id);

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id + 1L) : id + 1L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            batch.deleteRange(this.column, from, to);
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }

    public void getReferencesTo(int type, long id, @NonNull LongList dst) throws Exception {
        id = Element.addTypeToId(type, id);

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id + 1L) : id + 1L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            try (Slice toSlice = new Slice(to);
                 ReadOptions options = new ReadOptions(Database.READ_OPTIONS).setIterateUpperBound(toSlice);
                 RocksIterator iterator = this.database.delegate().newIterator(this.column, options)) {
                for (iterator.seek(from); iterator.isValid(); iterator.next()) {
                    dst.add(PUnsafe.getLong(iterator.key(), PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L));
                }
            }
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }
}
