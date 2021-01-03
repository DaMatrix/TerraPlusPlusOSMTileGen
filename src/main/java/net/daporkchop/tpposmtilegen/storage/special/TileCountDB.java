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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.WrappedRocksDB;
import org.rocksdb.Options;
import org.rocksdb.UInt64AddOperator;
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Tracks the number of tiles that an element belongs to.
 * <p>
 * struct Key {
 * long id; //the id of the element
 * };
 * <p>
 * Values are little-endian longs.
 *
 * @author DaPorkchop_
 */
public final class TileCountDB extends WrappedRocksDB {
    protected static final Options OPTIONS = new Options(DEFAULT_OPTIONS)
            .setMergeOperator(new UInt64AddOperator());

    public TileCountDB(@NonNull Path root, @NonNull String name) throws Exception {
        super(OPTIONS, root, name, 8);
    }

    public void setTileCount(int elementType, long element, long count) throws Exception {
        if (count == 0L) { //don't bother storing zeroes
            this.deleteTileCount(elementType, element);
            return;
        }

        WriteBatch batch = WRITE_BATCH_CACHE.get();
        try {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
            byte[] key = recycler.get();
            byte[] value = recycler.get();
            try {
                element = Element.addTypeToId(elementType, element);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(element) : element);
                PUnsafe.putLong(value, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(count) : count);

                batch.delete(key);
                batch.merge(key, value);
            } finally {
                recycler.release(value);
                recycler.release(key);
            }

            this.delegate.write(WRITE_OPTIONS, batch);
        } finally {
            batch.clear();
        }
    }

    public void changeTileCount(int elementType, long element, long delta) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        byte[] value = recycler.get();
        try {
            element = Element.addTypeToId(elementType, element);
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(element) : element);
            PUnsafe.putLong(value, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(delta) : delta);

            this.delegate.merge(WRITE_OPTIONS, key, value);
        } finally {
            recycler.release(value);
            recycler.release(key);
        }
    }

    public void deleteTileCount(int elementType, long element) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        try {
            element = Element.addTypeToId(elementType, element);
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(element) : element);

            this.delegate.delete(WRITE_OPTIONS, key);
        } finally {
            recycler.release(key);
        }
    }

    public long getTileCount(int elementType, long element) throws Exception {
        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = recycler.get();
        byte[] value = recycler.get();
        try {
            element = Element.addTypeToId(elementType, element);
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(element) : element);

            int cnt = this.delegate.get(READ_OPTIONS, key, value);
            if (cnt == 8) { //found
                return PUnsafe.getLong(value, PUnsafe.ARRAY_BYTE_BASE_OFFSET);
            } else if (cnt < 0) { //not found
                return 0L; //assume 0
            } else {
                throw new IllegalStateException("impossible value size: " + cnt);
            }
        } finally {
            recycler.release(value);
            recycler.release(key);
        }
    }

    public LongList getTileCounts(@NonNull LongList combinedIds) throws Exception {
        int size = combinedIds.size();
        if (size == 0) {
            return LongLists.EMPTY_LIST;
        }

        ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_8.get();
        List<byte[]> keys = new ArrayList<>(size);
        try {
            for (int i = 0; i < size; i++) {
                byte[] key = recycler.get();
                long combinedId = combinedIds.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
                keys.add(key);
            }

            List<byte[]> values = this.delegate.multiGetAsList(READ_OPTIONS, keys);

            LongList counts = new LongArrayList(size);
            for (int i = 0; i < size; i++) {
                byte[] value = values.get(i);
                long count = value != null ? PUnsafe.getLong(value, PUnsafe.ARRAY_BYTE_BASE_OFFSET) : 0L;
                counts.add(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(count) : count);
            }
            return counts;
        } finally {
            keys.forEach(recycler::release);
        }
    }
}
