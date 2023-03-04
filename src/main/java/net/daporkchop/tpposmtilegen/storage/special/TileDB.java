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
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.UInt64SetMergeOperator;
import net.daporkchop.tpposmtilegen.natives.UInt64ToBlobMapMergeOperator;
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
public class TileDB extends WrappedRocksDB {
    public TileDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void addElementToTiles(@NonNull DBWriteAccess access, @NonNull LongList tilePositions, long combinedId, @NonNull ByteBuf data) throws Exception {
        int size = tilePositions.size();
        if (size == 0) {
            return;
        }

        ByteBuffer key = DIRECT_BUFFER_RECYCLER_8.get().order(ByteOrder.BIG_ENDIAN);
        ByteBuf value = WRITE_BUFFER_CACHE.get().clear();
        UInt64ToBlobMapMergeOperator.add(value, combinedId, data);
        for (int i = 0; i < size; i++) {
            key.putLong(0, tilePositions.getLong(i));
            access.merge(this.column, (ByteBuffer) key.clear(), value.nioBuffer());
        }
    }

    public void deleteElementFromTiles(@NonNull DBWriteAccess access, @NonNull LongList tilePositions, long combinedId) throws Exception {
        int size = tilePositions.size();
        if (size == 0) {
            return;
        }

        ByteBuffer key = DIRECT_BUFFER_RECYCLER_8.get().order(ByteOrder.BIG_ENDIAN);
        ByteBuf value = WRITE_BUFFER_CACHE.get().clear();
        UInt64ToBlobMapMergeOperator.del(value, combinedId);
        for (int i = 0; i < size; i++) {
            key.putLong(0, tilePositions.getLong(i));
            access.merge(this.column, (ByteBuffer) key.clear(), value.nioBuffer());
        }
    }

    public void clearTile(@NonNull DBWriteAccess access, long tilePos) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), tilePos);
            access.delete(this.column, key);
        } finally {
            recycler.release(key);
        }
    }

    public void getElementsInTile(@NonNull DBReadAccess access, long tilePos, @NonNull LongObjConsumer<byte[]> callback) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), tilePos);
            byte[] arr = access.get(this.column, key);
            if (arr != null) {
                UInt64ToBlobMapMergeOperator.decodeToArrays(Unpooled.wrappedBuffer(arr), callback);
            }
        } finally {
            recycler.release(key);
        }
    }

    public static class Legacy extends TileDB {
        public Legacy(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
            super(database, column, desc);
        }

        @Override
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

        @Override
        public void deleteElementFromTiles(@NonNull DBWriteAccess access, @NonNull LongList tilePositions, long combinedId) throws Exception {
            int size = tilePositions.size();
            if (size == 0) {
                return;
            }

            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] key = recycler.get();
            try {
                PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(8), combinedId);
                for (int i = 0; i < size; i++) {
                    long id = tilePositions.getLong(i);
                    PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), id);
                    access.delete(this.column, key);
                }
            } finally {
                recycler.release(key);
            }
        }

        @Override
        public void clearTile(@NonNull DBWriteAccess access, long tilePos) throws Exception {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] from = recycler.get();
            byte[] to = recycler.get();
            try {
                PUnsafe.putUnalignedLongBE(from, PUnsafe.arrayByteElementOffset(0), tilePos);
                PUnsafe.putUnalignedLong(from, PUnsafe.arrayByteElementOffset(8), 0L);
                PUnsafe.putUnalignedLongBE(to, PUnsafe.arrayByteElementOffset(0), incrementExact(tilePos));
                PUnsafe.putUnalignedLong(to, PUnsafe.arrayByteElementOffset(8), 0L);
                access.deleteRange(this.column, from, to);
            } finally {
                recycler.release(from);
                recycler.release(to);
            }
        }

        @Override
        public void getElementsInTile(@NonNull DBReadAccess access, long tilePos, @NonNull LongObjConsumer<byte[]> callback) throws Exception {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] from = recycler.get();
            byte[] to = recycler.get();
            try {
                PUnsafe.putUnalignedLongBE(from, PUnsafe.arrayByteElementOffset(0), tilePos);
                PUnsafe.putUnalignedLong(from, PUnsafe.arrayByteElementOffset(8), 0L);
                PUnsafe.putUnalignedLongBE(to, PUnsafe.arrayByteElementOffset(0), incrementExact(tilePos));
                PUnsafe.putUnalignedLong(to, PUnsafe.arrayByteElementOffset(8), 0L);
                try (DBIterator iterator = access.iterator(this.column, from, to)) {
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                        byte[] key = iterator.key();

                        //iterating over a transaction can go far beyond the actual iteration bound (rocksdb bug), so we have to manually check
                        checkState(PUnsafe.getUnalignedLongBE(from, PUnsafe.arrayByteElementOffset(0)) == PUnsafe.getUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0)), "%d != %d",
                                PUnsafe.getUnalignedLongBE(from, PUnsafe.arrayByteElementOffset(0)), PUnsafe.getUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0)));

                        callback.accept(PUnsafe.getUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(8)), iterator.value());
                    }
                }
            } finally {
                recycler.release(from);
                recycler.release(to);
            }
        }
    }
}
