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

package net.daporkchop.tpposmtilegen.util.offheap;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IORunnable;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import net.daporkchop.tpposmtilegen.util.mmap.RefCountedMemoryMap;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

/**
 * @author DaPorkchop_
 */
public class OffHeapSpliteratableLongList implements AutoCloseable {
    private static final int THREAD_BUFFER_SIZE = 1 << 20;

    protected final FileChannel channel;
    protected final CloseableThreadLocal<ThreadBuffer> buffers = new CloseableThreadLocal<ThreadBuffer>() {
        @Override
        protected ThreadBuffer initialValue0() throws Exception {
            return new ThreadBuffer();
        }
    };

    public OffHeapSpliteratableLongList(@NonNull Path path) throws Exception {
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.channel.position(this.channel.size());
    }

    public void add(long value) throws Exception {
        this.buffers.get().add(value);
    }

    public synchronized void flush() throws Exception {
        this.buffers.forEach((EConsumer<ThreadBuffer>) this::write);
    }

    private synchronized void write(@NonNull ThreadBuffer tb) throws Exception {
        CompletableFuture.runAsync((IORunnable) () -> {
            ByteBuffer buffer = tb.buffer;
            buffer.flip();
            while (buffer.hasRemaining()) {
                this.channel.write(buffer);
            }
            buffer.clear();
        }).join();
    }

    @Override
    public void close() throws Exception {
        this.buffers.close();
        this.flush();

        this.channel.close();
    }

    public void clear() throws Exception {
        this.buffers.forEach(tb -> tb.buffer.clear());

        this.channel.truncate(0L);
    }

    public Spliterator.OfLong spliterator() throws Exception {
        /**
         * Initially copypasta'd from {@link java.util.stream.Streams#RangeLongSpliterator}.
         */
        @AllArgsConstructor
        class Impl implements Spliterator.OfLong {
            private static final long BALANCED_SPLIT_THRESHOLD = 1 << 24;
            private static final long RIGHT_BALANCED_SPLIT_RATIO = 1 << 3;

            @NonNull
            protected final RefCountedMemoryMap mmap;
            protected final long addr;

            private long from;
            private final long upTo;

            @Override
            public boolean tryAdvance(@NonNull LongConsumer consumer) {
                long i = this.from;
                if (i < this.upTo) {
                    this.from++;
                    long l = PUnsafe.getLong(this.addr + (i << 3L));
                    consumer.accept(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(l) : l);
                    return true;
                }
                return false;
            }

            @Override
            public void forEachRemaining(@NonNull LongConsumer consumer) {
                if (this.from < this.upTo) {
                    long i = this.from;
                    this.from = this.upTo;
                    while (i < this.upTo) {
                        long l = PUnsafe.getLong(this.addr + (i++ << 3L));
                        consumer.accept(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(l) : l);
                    }
                    this.mmap.release();
                }
            }

            @Override
            public long estimateSize() {
                return this.upTo - this.from;
            }

            @Override
            public int characteristics() {
                return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED |
                       Spliterator.IMMUTABLE | Spliterator.NONNULL;
            }

            @Override
            public Spliterator.OfLong trySplit() {
                long size = this.estimateSize();
                return size <= 1
                        ? null
                        // Left split always has a half-open range
                        : new Impl(this.mmap.retain(), this.addr, this.from, this.from = this.from + this.splitPoint(size));
            }

            private long splitPoint(long size) {
                long d = (size < BALANCED_SPLIT_THRESHOLD) ? 2 : RIGHT_BALANCED_SPLIT_RATIO;
                return size / d;
            }
        }

        RefCountedMemoryMap mmap = new RefCountedMemoryMap(this.channel, FileChannel.MapMode.READ_ONLY, 0L, this.channel.size());
        return new Impl(mmap, mmap.addr(), 0L, mmap.size() >>> 3L);
    }

    private final class ThreadBuffer implements AutoCloseable {
        protected final ByteBuffer buffer = ByteBuffer.allocateDirect(THREAD_BUFFER_SIZE);

        public void add(long value) throws Exception {
            this.buffer.putLong(value);

            if (!this.buffer.hasRemaining()) {
                this.close();
            }
        }

        @Override
        public void close() throws Exception {
            OffHeapSpliteratableLongList.this.write(this);
        }
    }
}
