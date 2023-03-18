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

package net.daporkchop.tpposmtilegen.storage.map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.primitive.lambda.LongLongConsumer;
import net.daporkchop.lib.primitive.lambda.LongLongObjConsumer;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.iterate.RocksColumnSpliterator;
import net.daporkchop.tpposmtilegen.util.DuplicatedList;
import net.daporkchop.tpposmtilegen.util.Threading;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public abstract class RocksDBMap<V> extends WrappedRocksDB {
    public RocksDBMap(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void put(@NonNull DBWriteAccess access, long key, @NonNull V value) throws Exception {
        ByteBuffer keyBuffer = DIRECT_BUFFER_RECYCLER_8.get();
        ByteBuf buf = WRITE_BUFFER_CACHE.get();

        keyBuffer.clear();
        keyBuffer.putLong(key).flip();

        this.valueToBytes(value, buf.clear());
        access.put(this.column, keyBuffer, buf.internalNioBuffer(0, buf.readableBytes()));
    }

    public boolean contains(@NonNull DBReadAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            //serialize key to bytes
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
            return access.contains(this.column, keyArray);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public void delete(@NonNull DBWriteAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            //serialize key to bytes
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
            access.delete(this.column, keyArray);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public V get(@NonNull DBReadAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);

            byte[] valueData = access.get(this.column, keyArray);
            return valueData != null ? this.valueFromBytes(key, Unpooled.wrappedBuffer(valueData)) : null;
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public List<V> getAll(@NonNull DBReadAccess access, @NonNull LongList keys) throws Exception {
        int size = keys.size();
        if (size == 0) {
            return Collections.emptyList();
        } else if (size
                   > 10000) { //split into smaller gets (prevents what i can only assume is a rocksdbjni bug where it will throw an NPE when requesting too many elements at once
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
                PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
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

    public static <V> List<V> getAll(@NonNull DBReadAccess access, @NonNull List<? extends RocksDBMap<V>> maps, @NonNull LongList keys) throws Exception {
        int size = keys.size();
        checkState(size == maps.size());

        if (size == 0) {
            return Collections.emptyList();
        } else if (maps instanceof DuplicatedList) { //multiple queries to the same column family
            return maps.get(0).getAll(access, keys);
        }

        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        List<byte[]> keyBytes = new ArrayList<>(size);
        List<byte[]> valueBytes;
        try {
            //retrieve column families
            ColumnFamilyHandle[] handles = maps.stream().map(map -> map.column).toArray(ColumnFamilyHandle[]::new);

            //serialize keys to bytes
            for (int i = 0; i < size; i++) {
                byte[] keyArray = keyArrayRecycler.get();
                long key = keys.getLong(i);
                PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
                keyBytes.add(keyArray);
            }

            //look up values from key
            valueBytes = access.multiGetAsList(Arrays.asList(handles), keyBytes);
        } finally {
            keyBytes.forEach(keyArrayRecycler::release);
        }

        //re-use list that was previously used for storing encoded keys and store deserialized values in it
        keyBytes.clear();
        List<V> values = uncheckedCast(keyBytes);

        for (int i = 0; i < size; i++) {
            byte[] value = valueBytes.get(i);
            values.add(value != null ? maps.get(i).valueFromBytes(keys.getLong(i), Unpooled.wrappedBuffer(value)) : null);
        }
        return values;
    }

    public void forEach(@NonNull DBReadAccess access, @NonNull LongObjConsumer<? super V> callback) throws Exception {
        try (DBIterator itr = access.iterator(this.column)) {
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                long key = PUnsafe.getUnalignedLongBE(itr.key(), PUnsafe.arrayByteElementOffset(0));
                callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(itr.value())));
            }
        }
    }

    public void forEachParallel(@NonNull DBReadAccess access, @NonNull LongObjConsumer<? super V> callback) throws Exception {
        if (access.isDirectRead()) {
            try (RocksColumnSpliterator rootSpliterator = new RocksColumnSpliterator(this.database, this.column, access.internalSnapshot(),
                    DatabaseConfig.ReadType.BULK_ITERATE, RocksColumnSpliterator.KeyOperations.FIXED_SIZE_LEX_ORDER)) {
                Threading.forEachParallel(CPU_COUNT, spliterator -> {
                    spliterator.forEachRemaining(slice -> {
                        checkArg(slice.keySize() == 8, slice.keySize());

                        long key = PUnsafe.getUnalignedLongBE(slice.keyAddr());
                        callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(slice.valueAddr(), slice.valueSize(), false)));
                    });
                }, rootSpliterator);
                return;
            }
        }

        @AllArgsConstructor
        class ValueWithKey {
            final byte[] key;
            final byte[] value;
        }

        Threading.<ValueWithKey>iterateParallel(32 * CPU_COUNT,
                c -> {
                    try (DBIterator itr = access.iterator(this.column)) {
                        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                            c.accept(new ValueWithKey(itr.key(), itr.value()));
                        }
                    }
                },
                v -> {
                    long key = PUnsafe.getUnalignedLongBE(v.key, PUnsafe.arrayByteElementOffset(0));
                    callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(v.value)));
                });
    }

    protected abstract void valueToBytes(@NonNull V value, @NonNull ByteBuf dst);

    protected abstract V valueFromBytes(long key, @NonNull ByteBuf valueBytes);

    public KeySpliterator keySpliterator() throws RocksDBException {
        return new KeySpliterator(new RocksColumnSpliterator(this.database, this.column, Optional.empty(),
                DatabaseConfig.ReadType.BULK_ITERATE, RocksColumnSpliterator.KeyOperations.FIXED_SIZE_LEX_ORDER));
    }

    public ValueSpliterator valueSpliterator() throws RocksDBException {
        return new ValueSpliterator(new RocksColumnSpliterator(this.database, this.column, Optional.empty(),
                DatabaseConfig.ReadType.BULK_ITERATE, RocksColumnSpliterator.KeyOperations.FIXED_SIZE_LEX_ORDER));
    }

    /**
     * @author DaPorkchop_
     */
    @RequiredArgsConstructor
    public static class KeySpliterator implements Spliterator.OfLong, AutoCloseable {
        @NonNull
        protected final RocksColumnSpliterator parent;

        public long smallestKeyInclusive() {
            return PUnsafe.getUnalignedLongBE(this.parent.smallestKeyInclusive(), PUnsafe.arrayByteElementOffset(0));
        }

        public long largestKeyInclusive() {
            return PUnsafe.getUnalignedLongBE(this.parent.largestKeyInclusive(), PUnsafe.arrayByteElementOffset(0));
        }

        public OptionalLong largestKeyExclusive() {
            return this.parent.largestKeyExclusive() != null
                    ? OptionalLong.of(PUnsafe.getUnalignedLongBE(this.parent.largestKeyExclusive(), PUnsafe.arrayByteElementOffset(0)))
                    : OptionalLong.empty();
        }

        @Override
        public void close() throws Exception {
            this.parent.close();
        }

        @Override
        public KeySpliterator trySplit() {
            RocksColumnSpliterator split = this.parent.trySplit();
            return split != null ? new KeySpliterator(split) : null;
        }

        @Override
        public boolean tryAdvance(@NonNull LongConsumer action) {
            return this.parent.tryAdvance(slice -> {
                checkState(slice.keySize() == 8);
                action.accept(PUnsafe.getUnalignedLongBE(slice.keyAddr()));
            });
        }

        @Override
        public void forEachRemaining(@NonNull LongConsumer action) {
            this.parent.forEachRemaining(slice -> {
                checkState(slice.keySize() == 8);
                action.accept(PUnsafe.getUnalignedLongBE(slice.keyAddr()));
            });
        }

        @Override
        public long estimateSize() {
            return this.parent.estimateSize();
        }

        @Override
        public int characteristics() {
            return this.parent.characteristics() | SORTED;
        }

        @Override
        public long getExactSizeIfKnown() {
            return this.parent.getExactSizeIfKnown();
        }
    }

    /**
     * @author DaPorkchop_
     */
    @RequiredArgsConstructor
    public class ValueSpliterator implements Spliterator<V>, AutoCloseable {
        @NonNull
        protected final RocksColumnSpliterator parent;

        public long smallestKeyInclusive() {
            return PUnsafe.getUnalignedLongBE(this.parent.smallestKeyInclusive(), PUnsafe.arrayByteElementOffset(0));
        }

        public long largestKeyInclusive() {
            return PUnsafe.getUnalignedLongBE(this.parent.largestKeyInclusive(), PUnsafe.arrayByteElementOffset(0));
        }

        public OptionalLong largestKeyExclusive() {
            return this.parent.largestKeyExclusive() != null
                    ? OptionalLong.of(PUnsafe.getUnalignedLongBE(this.parent.largestKeyExclusive(), PUnsafe.arrayByteElementOffset(0)))
                    : OptionalLong.empty();
        }

        @Override
        public void close() throws Exception {
            this.parent.close();
        }

        @Override
        public ValueSpliterator trySplit() {
            RocksColumnSpliterator split = this.parent.trySplit();
            return split != null ? new ValueSpliterator(split) : null;
        }

        @Override
        public boolean tryAdvance(@NonNull Consumer<? super V> action) {
            return this.parent.tryAdvance(slice -> {
                checkState(slice.keySize() == 8);
                action.accept(RocksDBMap.this.valueFromBytes(
                        PUnsafe.getUnalignedLongBE(slice.keyAddr()),
                        Unpooled.wrappedBuffer(slice.valueAddr(), slice.valueSize(), false)));
            });
        }

        @Override
        public void forEachRemaining(@NonNull Consumer<? super V> action) {
            this.parent.forEachRemaining(slice -> {
                checkState(slice.keySize() == 8);
                action.accept(RocksDBMap.this.valueFromBytes(
                        PUnsafe.getUnalignedLongBE(slice.keyAddr()),
                        Unpooled.wrappedBuffer(slice.valueAddr(), slice.valueSize(), false)));
            });
        }

        @Override
        public long estimateSize() {
            return this.parent.estimateSize();
        }

        @Override
        public int characteristics() {
            return this.parent.characteristics();
        }

        @Override
        public long getExactSizeIfKnown() {
            return this.parent.getExactSizeIfKnown();
        }
    }
}
