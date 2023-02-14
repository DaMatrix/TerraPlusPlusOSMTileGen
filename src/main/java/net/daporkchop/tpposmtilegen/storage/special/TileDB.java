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

package net.daporkchop.tpposmtilegen.storage.special;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

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

    public void addElementToTiles(@NonNull DBWriteAccess access, @NonNull LongList tilePositions, long combinedId, @NonNull ByteBuf data) throws Exception {
        int size = tilePositions.size();
        if (size == 0) {
            return;
        }

        ByteBuffer key = DIRECT_BUFFER_RECYCLER_16.get().order(ByteOrder.BIG_ENDIAN);
        key.putLong(8, combinedId);
        for (int i = 0; i < size; i++) {
            key.putLong(0, tilePositions.getLong(i));
            access.put(this.column, (ByteBuffer) key.clear(), data.nioBuffer());
        }
    }

    public void deleteElementFromTiles(@NonNull DBWriteAccess access, @NonNull LongList tilePositions, long combinedId) throws Exception {
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

    public void clearTile(@NonNull DBWriteAccess access, long tilePos) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(incrementExact(tilePos)) : incrementExact(tilePos));
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            access.deleteRange(this.column, from, to);
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }

    public void getElementsInTile(@NonNull DBReadAccess access, long tilePos, @NonNull LongObjConsumer<byte[]> callback) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(incrementExact(tilePos)) : incrementExact(tilePos));
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            try (DBIterator iterator = access.iterator(this.column, from, to)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();

                    //iterating over a transaction can go far beyond the actual iteration bound (rocksdb bug), so we have to manually check
                    checkState(PUnsafe.getLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET) == PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET), "%d != %d",
                            PUnsafe.getLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET), PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET));

                    long combinedId = PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L);
                    if (PlatformInfo.IS_LITTLE_ENDIAN) {
                        combinedId = Long.reverseBytes(combinedId);
                    }
                    callback.accept(combinedId, iterator.value());
                }
            }
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }
}
