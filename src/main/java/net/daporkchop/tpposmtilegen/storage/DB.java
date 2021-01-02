/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapAtomicLong;
import net.daporkchop.tpposmtilegen.util.persistent.PersistentMap;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public abstract class DB<V> implements PersistentMap<V> {
    private static final Ref<KeyArrayRecycler> KEY_ARRAY_RECYCLER = ThreadRef.soft(KeyArrayRecycler::new);
    protected static final Ref<ByteBuf> WRITE_BUFFER_CACHE = ThreadRef.late(UnpooledByteBufAllocator.DEFAULT::heapBuffer);
    protected static final CloseableThreadLocal<WriteBatch> WRITE_BATCH_CACHE = CloseableThreadLocal.of(WriteBatch::new);

    protected static final Options OPTIONS;
    protected static final ReadOptions READ_OPTIONS;
    protected static final WriteOptions WRITE_OPTIONS;
    protected static final FlushOptions FLUSH_OPTIONS;

    static {
        RocksDB.loadLibrary(); //ensure rocksdb native library is loaded before creating options instances

        OPTIONS = new Options()
                .setCreateIfMissing(true)
                .setArenaBlockSize(1L << 20)
                .setOptimizeFiltersForHits(true)
                .setSkipStatsUpdateOnDbOpen(true)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCompactionReadaheadSize(4L << 20L)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setAllowConcurrentMemtableWrite(true)
                .setIncreaseParallelism(CPU_COUNT)
                .setMaxOpenFiles(-1)
                .setAllowMmapReads(true)
                .setAllowMmapWrites(true);
        //.setMergeOperatorName("sortlist")

        READ_OPTIONS = new ReadOptions();
        WRITE_OPTIONS = new WriteOptions();
        FLUSH_OPTIONS = new FlushOptions()
                .setWaitForFlush(true);
    }

    protected final Options options;
    protected final RocksDB delegate;

    public DB(@NonNull Path root, @NonNull String name) throws Exception {
        this(OPTIONS, root, name);
    }

    public DB(@NonNull Options options, @NonNull Path root, @NonNull String name) throws Exception {
        this.options = options;
        this.delegate = RocksDB.open(options, root.resolve(name).toString());
    }

    @Override
    @Deprecated
    public void put(long key, @NonNull V value) throws Exception {
        ByteBuf buf = WRITE_BUFFER_CACHE.get().clear();
        buf.writeLong(key);
        this.valueToBytes(value, buf);
        int valueSize = buf.readableBytes() - 8;

        byte[] arr = buf.array();
        int offset = buf.arrayOffset();
        this.delegate.put(WRITE_OPTIONS,
                arr, offset, 8,
                arr, offset + 8, valueSize);
    }

    @Override
    public void putAll(@NonNull LongList keys, @NonNull List<V> values) throws Exception {
        checkArg(keys.size() == values.size(), "must have same number of keys as values!");
        int size = keys.size();

        WriteBatch batch = WRITE_BATCH_CACHE.get();
        batch.clear(); //ensure write batch is empty

        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(8);
        try {
            ByteBuf buf = WRITE_BUFFER_CACHE.get();

            for (int i = 0; i < size; i++) {
                keyBuffer.clear();
                keyBuffer.putInt(i).flip();

                this.valueToBytes(values.get(i), buf.clear());
                batch.put(keyBuffer, buf.internalNioBuffer(0, buf.readableBytes()));
            }
        } finally {
            PUnsafe.pork_releaseBuffer(keyBuffer);
        }

        this.delegate.write(WRITE_OPTIONS, batch);
    }

    @Override
    public V get(long key) throws Exception {
        KeyArrayRecycler keyArrayRecycler = KEY_ARRAY_RECYCLER.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putLong(keyArray, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(key) : key);

            byte[] valueData = this.delegate.get(READ_OPTIONS, keyArray);
            return valueData != null ? this.valueFromBytes(key, Unpooled.wrappedBuffer(valueData)) : null;
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    @Override
    public List<V> getAll(@NonNull LongList keys) throws Exception {
        int size = keys.size();
        if (size == 0) {
            return Collections.emptyList();
        }

        KeyArrayRecycler keyArrayRecycler = KEY_ARRAY_RECYCLER.get();
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
            valueBytes = this.delegate.multiGetAsList(keyBytes);
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

    @Override
    public void flush() throws Exception {
        this.delegate.flush(FLUSH_OPTIONS);
    }

    @Override
    public void close() throws Exception {
        this.delegate.flush(FLUSH_OPTIONS);
        this.delegate.close();
    }

    protected abstract void valueToBytes(@NonNull V value, @NonNull ByteBuf dst);

    protected abstract V valueFromBytes(long key, @NonNull ByteBuf valueBytes);

    private static final class KeyArrayRecycler extends SimpleRecycler<byte[]> {
        @Override
        protected byte[] newInstance0() {
            return new byte[8];
        }

        @Override
        protected void reset0(@NonNull byte[] value) {
            //no-op
        }

        @Override
        protected boolean hasCapacity() {
            return this.size() < 20_000; //don't cache more than 20k arrays
        }
    }
}
