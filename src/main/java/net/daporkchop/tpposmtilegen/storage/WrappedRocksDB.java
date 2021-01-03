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
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.util.Arrays;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public abstract class WrappedRocksDB {
    protected static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    protected static final Ref<ByteArrayRecycler> BYTE_ARRAY_RECYCLER_8 = ThreadRef.soft(() -> new ByteArrayRecycler(8));
    protected static final Ref<ByteArrayRecycler> BYTE_ARRAY_RECYCLER_16 = ThreadRef.soft(() -> new ByteArrayRecycler(16));
    protected static final Ref<ByteBuf> WRITE_BUFFER_CACHE = ThreadRef.late(UnpooledByteBufAllocator.DEFAULT::directBuffer);
    protected static final CloseableThreadLocal<WriteBatch> WRITE_BATCH_CACHE = CloseableThreadLocal.of(WriteBatch::new);

    protected static final Options DEFAULT_OPTIONS;
    protected static final ReadOptions READ_OPTIONS;
    protected static final WriteOptions WRITE_OPTIONS;
    protected static final WriteOptions SYNC_WRITE_OPTIONS;

    static {
        RocksDB.loadLibrary(); //ensure rocksdb native library is loaded before creating options instances

        DEFAULT_OPTIONS = new Options()
                .setCreateIfMissing(true)
                .setArenaBlockSize(1L << 20)
                .setOptimizeFiltersForHits(true)
                .setSkipStatsUpdateOnDbOpen(true)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCompactionReadaheadSize(16L << 20L)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setAllowConcurrentMemtableWrite(true)
                .setIncreaseParallelism(CPU_COUNT)
                .setMaxSubcompactions(CPU_COUNT)
                .setCompactionOptionsUniversal(new CompactionOptionsUniversal()
                        .setAllowTrivialMove(true))
                .setKeepLogFileNum(2L)
                .setMaxOpenFiles(-1)
                .setAllowMmapReads(true)
                .setAllowMmapWrites(true);

        READ_OPTIONS = new ReadOptions();
        WRITE_OPTIONS = new WriteOptions();
        SYNC_WRITE_OPTIONS = new WriteOptions(WRITE_OPTIONS).setSync(true);
    }

    protected final Options options;
    protected final RocksDB delegate;

    protected final byte[] lowKey;
    protected final byte[] highKey;
    protected final int keySize;

    public WrappedRocksDB(@NonNull Options options, @NonNull Path root, @NonNull String name, int keySize) throws Exception {
        this.options = options;
        this.keySize = positive(keySize);
        this.delegate = RocksDB.open(options, root.resolve(name).toString());

        this.initializeKeyRanges(this.lowKey = new byte[keySize], this.highKey = new byte[keySize]);
    }

    protected void initializeKeyRanges(@NonNull byte[] lowKey, @NonNull byte[] highKey) {
        Arrays.fill(lowKey, (byte) 0);
        Arrays.fill(highKey, (byte) 0xFF);
    }

    public void clear() throws Exception {
        WriteBatch batch = WRITE_BATCH_CACHE.get();
        try {
            batch.deleteRange(this.lowKey, this.highKey);
            batch.delete(this.highKey); //deleteRange's upper bound is exclusive

            this.delegate.write(SYNC_WRITE_OPTIONS, batch);
            this.delegate.compactRange(); //force compaction to delete all table files
        } finally {
            batch.clear();
        }
    }

    public void flush() throws Exception {
    }

    public void close() throws Exception {
        this.delegate.close();
    }

    @RequiredArgsConstructor
    protected static final class ByteArrayRecycler extends SimpleRecycler<byte[]> {
        protected final int size;

        @Override
        protected byte[] newInstance0() {
            return new byte[this.size];
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
