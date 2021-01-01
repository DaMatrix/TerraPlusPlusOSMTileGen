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
import lombok.NonNull;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
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
public abstract class DB<K, V> implements PersistentMap<K, V> {
    protected static final Ref<ByteBuf[]> WRITE_BUFFER_CACHE = ThreadRef.late(() -> new ByteBuf[]{
            UnpooledByteBufAllocator.DEFAULT.buffer(),
            UnpooledByteBufAllocator.DEFAULT.buffer()
    });
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
    protected final OffHeapAtomicLong maxValueSize;

    public DB(@NonNull Path root, @NonNull String name) throws Exception {
        this(OPTIONS, root, name);
    }

    public DB(@NonNull Options options, @NonNull Path root, @NonNull String name) throws Exception {
        this.options = options;
        this.delegate = RocksDB.open(options, root.resolve(name).toString());

        this.maxValueSize = new OffHeapAtomicLong(root.resolve(name + "_maxValueSize"), 0L);
    }

    @Override
    public void put(@NonNull K key, @NonNull V value) throws Exception {
        ByteBuf[] bufs = WRITE_BUFFER_CACHE.get();
        this.keyToBytes(key, bufs[0].clear());
        this.valueToBytes(value, bufs[1].clear());
        int valueSize = bufs[1].readableBytes();

        this.maxValueSize.max(valueSize);
        this.delegate.put(WRITE_OPTIONS,
                bufs[0].internalNioBuffer(0, bufs[0].readableBytes()),
                bufs[1].internalNioBuffer(0, valueSize));
    }

    @Override
    public void putAll(@NonNull List<K> keys, @NonNull List<V> values) throws Exception {
        checkArg(keys.size() == values.size(), "must have same number of keys as values!");

        WriteBatch batch = WRITE_BATCH_CACHE.get();
        batch.clear(); //ensure write batch is empty

        ByteBuf[] bufs = WRITE_BUFFER_CACHE.get();

        int maxValueSize = 0;
        for (int i = 0; i < keys.size(); i++) {
            this.keyToBytes(keys.get(i), bufs[0].clear());
            this.valueToBytes(values.get(i), bufs[1].clear());
            int valueSize = bufs[1].readableBytes();
            maxValueSize = max(maxValueSize, valueSize);

            batch.put(
                    bufs[0].internalNioBuffer(0, bufs[0].readableBytes()),
                    bufs[1].internalNioBuffer(0, valueSize));
        }

        this.maxValueSize.max(maxValueSize);
        this.delegate.write(WRITE_OPTIONS, batch);
    }

    @Override
    public V get(@NonNull K key) throws Exception {
        ByteBuf[] bufs = WRITE_BUFFER_CACHE.get();
        this.keyToBytes(key, bufs[0].clear());

        int maxValueSize = toInt(this.maxValueSize.get());
        int valueSize = this.delegate.get(READ_OPTIONS,
                bufs[0].internalNioBuffer(0, bufs[0].readableBytes()),
                bufs[1].clear().ensureWritable(maxValueSize).internalNioBuffer(0, maxValueSize));
        return valueSize >= 0
                ? this.valueFromBytes(bufs[0], bufs[1].writerIndex(valueSize)) //success
                : null; //error
    }

    @Override
    public List<V> getAll(@NonNull List<K> keys) throws Exception {
        int size = keys.size();
        if (size == 0) {
            return Collections.emptyList();
        }

        ByteBuf[] bufs = WRITE_BUFFER_CACHE.get();
        List<byte[]> keyBytes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            this.keyToBytes(keys.get(i), bufs[0].clear());
            byte[] arr = new byte[bufs[0].readableBytes()];
            bufs[0].readBytes(arr);
            keyBytes.add(arr);
        }

        List<byte[]> valueBytes = this.delegate.multiGetAsList(keyBytes);
        keyBytes.clear();
        List<V> values = uncheckedCast(keyBytes); //re-use list that was previously used for storing encoded keys

        for (int i = 0; i < size; i++) {
            byte[] value = valueBytes.get(i);
            values.add(value != null ? this.valueFromBytes(keys.get(i), Unpooled.wrappedBuffer(value)) : null);
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
        this.maxValueSize.close();
    }

    protected abstract void keyToBytes(@NonNull K key, @NonNull ByteBuf dst);

    protected abstract void valueToBytes(@NonNull V value, @NonNull ByteBuf dst);

    protected abstract V valueFromBytes(@NonNull K key, @NonNull ByteBuf valueBytes);

    protected abstract V valueFromBytes(@NonNull ByteBuf keyBytes, @NonNull ByteBuf valueBytes);
}
