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

package net.daporkchop.tpposmtilegen.storage;

import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.util.Point;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.Arrays;

/**
 * @author DaPorkchop_
 */
public final class TileDB implements AutoCloseable {
    public static final int TILES_PER_DEGREE = 64;

    public static int toTileCoordinate(int pointCoordinate) {
        return Math.floorDiv(pointCoordinate, Point.PRECISION / TILES_PER_DEGREE);
    }

    public static long toTilePosition(int x, int y) {
        return interleaveBits(toTileCoordinate(x), toTileCoordinate(y));
    }

    public static long interleaveBits(int x, int y) {
        x = (x << 1) ^ (x >> 31); //ZigZag encoding
        y = (y << 1) ^ (y >> 31);

        long l = 0L;
        for (int i = 0; i < 32; i++) {
            l |= ((long) (x & (1 << i)) << i) | ((long) (y & (1 << i)) << (i + 1));
        }
        return l;
    }

    private static int uninterleave(long l) {
        int i = 0;
        for (int j = 0; j < 32; j++) {
            i |= ((l >>> (j << 1)) & 1) << j;
        }
        return (i >> 1) ^ -(i & 1);
    }

    public static int extractTileX(long tilePos) {
        return uninterleave(tilePos);
    }

    public static int extractTileY(long tilePos) {
        return uninterleave(tilePos >>> 1L);
    }

    protected final RocksDB delegate;

    public TileDB(@NonNull Path root, @NonNull String name) throws Exception {
        this.delegate = RocksDB.open(DB.OPTIONS, root.resolve(name).toString());
    }

    public void addElementToTiles(@NonNull LongList tilePositions, int elementType, long element) throws Exception {
        int size = tilePositions.size();
        if (size == 0) {
            return;
        }

        WriteBatch batch = DB.WRITE_BATCH_CACHE.get();
        batch.clear(); //ensure write batch is empty

        DB.ByteArrayRecycler recycler = DB.BYTE_ARRAY_RECYCLER_16.get();
        byte[] key = recycler.get();
        try {
            element = Element.addTypeToId(element, elementType);
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(element) : element);
            for (int i = 0; i < size; i++) {
                long id = tilePositions.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                batch.put(key, DB.EMPTY_BYTE_ARRAY);
            }
        } finally {
            recycler.release(key);
        }

        this.delegate.write(DB.WRITE_OPTIONS, batch);
    }

    public void clearTile(int tileX, int tileY) throws Exception {
        this.clearTile(interleaveBits(tileX, tileY));
    }

    public void clearTile(long tilePos) throws Exception {
        DB.ByteArrayRecycler recycler = DB.BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos + 1L) : tilePos + 1L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            this.delegate.deleteRange(DB.WRITE_OPTIONS, from, to);
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }

    public void getElementsInTile(int tileX, int tileY, @NonNull LongList dst) throws Exception {
        this.getElementsInTile(interleaveBits(tileX, tileY), dst);
    }

    public void getElementsInTile(long tilePos, @NonNull LongList dst) throws Exception {
        DB.ByteArrayRecycler recycler = DB.BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos) : tilePos);
            PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(tilePos + 1L) : tilePos + 1L);
            PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
            try (Slice toSlice = new Slice(to);
                 ReadOptions options = new ReadOptions(DB.READ_OPTIONS).setIterateUpperBound(toSlice);
                 RocksIterator iterator = this.delegate.newIterator(options)) {
                for (iterator.seek(from); iterator.isValid(); iterator.next()) {
                    long val = PUnsafe.getLong(iterator.key(), PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L);
                    dst.add(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(val) : val);
                }
            }
        } finally {
            recycler.release(from);
            recycler.release(to);
        }
    }

    public void clear() throws Exception {
        WriteBatch batch = DB.WRITE_BATCH_CACHE.get();
        batch.clear(); //ensure write batch is empty

        DB.ByteArrayRecycler recycler = DB.BYTE_ARRAY_RECYCLER_16.get();
        byte[] from = recycler.get();
        byte[] to = recycler.get();
        try {
            Arrays.fill(from, (byte) 0);
            Arrays.fill(to, (byte) 0xFF);
            batch.deleteRange(from, to);
            batch.delete(to); //range upper bound is exclusive, so delete that one as well just in case
        } finally {
            recycler.release(from);
            recycler.release(to);
        }

        this.delegate.write(DB.SYNC_WRITE_OPTIONS, batch);
        this.delegate.compactRange();
    }

    public void flush() throws Exception {
    }

    @Override
    public void close() throws Exception {
        this.delegate.close();
    }
}
