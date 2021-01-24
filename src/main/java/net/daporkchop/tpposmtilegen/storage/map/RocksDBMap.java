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

package net.daporkchop.tpposmtilegen.storage.map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.util.DuplicatedList;
import net.daporkchop.tpposmtilegen.util.Threading;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public abstract class RocksDBMap<V> extends WrappedRocksDB {
    public RocksDBMap(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void put(@NonNull DBAccess access, long key, @NonNull V value) throws Exception {
        ByteBuffer keyBuffer = DIRECT_KEY_BUFFER_CACHE.get();
        ByteBuf buf = WRITE_BUFFER_CACHE.get();

        keyBuffer.clear();
        keyBuffer.putLong(key).flip();

        this.valueToBytes(value, buf.clear());
        access.put(this.column, keyBuffer, buf.internalNioBuffer(0, buf.readableBytes()));
    }

    public void delete(@NonNull DBAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            //serialize key to bytes
            PUnsafe.putLong(keyArray, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(key) : key);
            access.delete(this.column, keyArray);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public V get(@NonNull DBAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putLong(keyArray, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(key) : key);

            byte[] valueData = access.get(this.column, keyArray);
            return valueData != null ? this.valueFromBytes(key, Unpooled.wrappedBuffer(valueData)) : null;
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public List<V> getAll(@NonNull DBAccess access, @NonNull LongList keys) throws Exception {
        int size = keys.size();
        if (size == 0) {
            return Collections.emptyList();
        } else if (size > 10000) { //split into smaller gets (prevents what i can only assume is a rocksdbjni bug where it will throw an NPE when requesting too many elements at once
            List<V> dst = new ArrayList<>(size);
            for (int i = 0; i < size; i += 10000) {
                dst.addAll(this.getAll(access, keys.subList(i, min(i + 10000, size))));
            }
            return dst;
        }

        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        List<byte[]> keyBytes = new ArrayList<>(size);
        List<byte[]> valueBytes;
        try {
            //serialize keys to bytes
            for (int i = 0; i < size; i++) {
                byte[] keyArray = keyArrayRecycler.get();
                long key = keys.getLong(i);
                PUnsafe.putLong(keyArray, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(key) : key);
                keyBytes.add(keyArray);
            }

            //look up values from key
            valueBytes = access.multiGetAsList(new DuplicatedList<>(this.column, size), keyBytes);
        } finally {
            keyBytes.forEach(keyArrayRecycler::release);
        }

        //re-use list that was previously used for storing encoded keys and store deserialized values in it
        keyBytes.clear();
        List<V> values = uncheckedCast(keyBytes);

        for (int i = 0; i < size; i++) {
            byte[] value = valueBytes.get(i);
            values.add(value != null ? this.valueFromBytes(keys.getLong(i), Unpooled.wrappedBuffer(value)) : null);
        }
        return values;
    }

    public void forEach(@NonNull DBAccess access, @NonNull LongObjConsumer<? super V> callback) throws Exception {
        try (RocksIterator itr = access.iterator(this.column)) {
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                long key = PUnsafe.getLong(itr.key(), PUnsafe.ARRAY_BYTE_BASE_OFFSET);
                if (PlatformInfo.IS_LITTLE_ENDIAN) {
                    key = Long.reverseBytes(key);
                }
                callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(itr.value())));
            }
        }
    }

    public void forEachParallel(@NonNull DBAccess access, @NonNull LongObjConsumer<? super V> callback) throws Exception {
        @AllArgsConstructor
        class ValueWithKey {
            final byte[] key;
            final byte[] value;
        }

        Threading.<ValueWithKey>iterateParallel(1024,
                c -> {
                    try (RocksIterator itr = access.iterator(this.column)) {
                        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                            c.accept(new ValueWithKey(itr.key(), itr.value()));
                        }
                    }
                },
                v -> {
                    long key = PUnsafe.getLong(v.key, PUnsafe.ARRAY_BYTE_BASE_OFFSET);
                    if (PlatformInfo.IS_LITTLE_ENDIAN) {
                        key = Long.reverseBytes(key);
                    }
                    callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(v.value)));
                });
    }

    public Long highestId(@NonNull DBAccess access) throws Exception {
        try (RocksIterator itr = access.iterator(this.column)) {
            itr.seekToLast();
            if (itr.isValid()) {
                long key = PUnsafe.getLong(itr.key(), PUnsafe.ARRAY_BYTE_BASE_OFFSET);
                if (PlatformInfo.IS_LITTLE_ENDIAN) {
                    key = Long.reverseBytes(key);
                }
                return key;
            } else {
                return null;
            }
        }
    }

    protected abstract void valueToBytes(@NonNull V value, @NonNull ByteBuf dst);

    protected abstract V valueFromBytes(long key, @NonNull ByteBuf valueBytes);
}
