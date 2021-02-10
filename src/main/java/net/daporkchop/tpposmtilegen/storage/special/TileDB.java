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

import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Tracks which elements are contained in each tile.
 * <p>
 * struct Key {
 * long tilePos; //packed tile position
 * long elementId; //the id of the element
 * };
 *
 * @author DaPorkchop_
 */
public final class TileDB extends WrappedRocksDB {
    public TileDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void addElementToTiles(@NonNull DBAccess access, @NonNull LongList tilePositions, long combinedId) throws Exception {
        int size = tilePositions.size();
        if (size == 0) {
            return;
        }

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
            for (int i = 0; i < size; i++) {
                long tilePos = tilePositions.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
                access.put(this.column, key, EMPTY_BYTE_ARRAY);
            }
        } finally {
            recycler.release(key);
        }
    }

    public void deleteElementFromTiles(@NonNull DBAccess access, @NonNull LongList tilePositions, long combinedId) throws Exception {
        int size = tilePositions.size();
        if (size == 0) {
            return;
        }

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
            for (int i = 0; i < size; i++) {
                long id = tilePositions.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                access.delete(this.column, key);
            }
        } finally {
            recycler.release(key);
        }
    }

    public void clearTile(@NonNull DBAccess access, long tilePos) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos + 1L) : tilePos + 1L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            access.deleteRange(this.column, from, to);
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }

    public void getElementsInTile(@NonNull DBAccess access, long tilePos, @NonNull LongCollection dst) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos + 1L) : tilePos + 1L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            try (Slice toSlice = new Slice(to);
                 ReadOptions options = new ReadOptions(Database.READ_OPTIONS).setIterateUpperBound(toSlice);
                 RocksIterator iterator = access.iterator(this.column, options)) {
                for (iterator.seek(from); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();

                    if (PUnsafe.getLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET) != PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET)) {
                        //iterating over a transaction can go far beyond the actual iteration bound (rocksdb bug), so we have to manually check
                        return;
                    }

                    long val = PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L);
                    dst.add(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(val) : val);
                }
            }
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }
}
