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
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.MemoryHelper;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.sparse.AbstractSparseAllocator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Spliterator;
import java.util.function.LongConsumer;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * An off-heap, memory-mapped bitset which supports 64-bit indexing.
 *
 * @author DaPorkchop_
 */
public class OffHeapAtomicBitSet extends AbstractSparseAllocator {
    private final long base = super.headerSize();

    public OffHeapAtomicBitSet(@NonNull Path path, long size) throws IOException {
        super(path, size);
    }

    public boolean get(long index) {
        long addr = this.addr;
        long wordIndex = index >>> 6L;
        long currSize = PUnsafe.getLong(addr + this.base);
        if (wordIndex > currSize) {
            return false;
        }
        return (PUnsafe.getLong(addr + this.base + 8L + (wordIndex << 3L)) & (1L << (index & 0x3FL))) != 0L;
    }

    public boolean set(long index) {
        long wordIndex = index >>> 6L;
        this.ensureCapacity(wordIndex);

        long wordAddr = this.addr + this.base + 8L + (wordIndex << 3L);
        long val;
        do {
            val = PUnsafe.getLongVolatile(null, wordAddr);
        } while (!PUnsafe.compareAndSwapLong(null, wordAddr, val, val | (1L << (index & 0x3FL))));
        return (val & (1L << (index & 0x3FL))) != 0L;
    }

    public void clear(long index) {
        long wordIndex = index >>> 6L;
        this.ensureCapacity(wordIndex);

        long wordAddr = this.addr + this.base + 8L + (wordIndex << 3L);
        long val;
        do {
            val = PUnsafe.getLongVolatile(null, wordAddr);
        } while (!PUnsafe.compareAndSwapLong(null, wordAddr, val, val & ~(1L << (index & 0x3FL))));
    }

    protected void ensureCapacity(long wordIndex) {
        long addr = this.addr;
        long currSize = PUnsafe.getLongVolatile(null, addr + this.base);
        if (wordIndex > currSize) { //fill unallocated space with zeroes and then increase size pointer
            synchronized (this) {
                currSize = PUnsafe.getLongVolatile(null, addr + this.base);
                if (wordIndex > currSize) {
                    //fill unallocated space with zeroes
                    long fillStart = addr + this.base + 8L + (currSize << 3L) + 8L;
                    long fillCount = (wordIndex - currSize + 8L) << 3L;
                    PUnsafe.setMemory(fillStart, fillCount, (byte) 0);
                    //update size marker
                    PUnsafe.putLongVolatile(null, addr + this.base, wordIndex);
                }
            }
        }
    }

    public void set(long fromIndex, long toIndex) { //adapted from java.util.BitSet
        checkArg(toIndex >= fromIndex, "toIndex (%d) may not be less than fromIndex (%d)", toIndex, fromIndex);
        if (fromIndex == toIndex) { //empty range
            return;
        }

        long startWordIndex = fromIndex >>> 6L << 3L;
        long endWordIndex = (toIndex - 1L) >>> 6L << 3L;
        this.ensureCapacity(endWordIndex >> 3L); //ensure capacity up to higher index

        long firstWordMask = -1L << fromIndex;
        long lastWordMask = -1L >>> -toIndex;
        long val;
        long dataBase = this.addr + this.base + 8L;
        if (startWordIndex == endWordIndex) {
            // Case 1: One word
            do {
                val = PUnsafe.getLongVolatile(null, dataBase + startWordIndex);
            } while (!PUnsafe.compareAndSwapLong(null, dataBase + startWordIndex, val, val | (firstWordMask & lastWordMask)));
        } else {
            // Case 2: Multiple words
            // Handle first word
            do {
                val = PUnsafe.getLongVolatile(null, dataBase + startWordIndex);
            } while (!PUnsafe.compareAndSwapLong(null, dataBase + startWordIndex, val, val | firstWordMask));

            // Handle intermediate words, if any
            for (long i = startWordIndex + 8L; i < endWordIndex; i += 8L) {
                do {
                    val = PUnsafe.getLongVolatile(null, dataBase + i);
                } while (!PUnsafe.compareAndSwapLong(null, dataBase + i, val, -1L));
            }

            // Handle last word (restores invariants)
            do {
                val = PUnsafe.getLongVolatile(null, dataBase + endWordIndex);
            } while (!PUnsafe.compareAndSwapLong(null, dataBase + endWordIndex, val, val | lastWordMask));
        }
    }

    public void clear() {
        long addr = this.addr;
        long currSize = PUnsafe.getLongVolatile(null, addr + this.base);
        if (currSize > 0L) {
            synchronized (this) {
                currSize = PUnsafe.getLongVolatile(null, addr + this.base);
                if (currSize > 0L) {
                    //fill with zeroes
                    long fillStart = addr + this.base + 8L;
                    long fillCount = currSize << 3L;
                    PUnsafe.setMemory(fillStart, fillCount, (byte) 0);
                    //update size marker
                    PUnsafe.putLongVolatile(null, addr + this.base, -1L);
                }
            }
        }
    }

    @Override
    protected long headerSize() {
        return super.headerSize() + 8L;
    }

    @Override
    protected void initHeaders() {
        super.initHeaders();

        PUnsafe.putLong(this.addr + super.headerSize(), -1L);
    }

    @Override
    protected long actualSize0() {
        return this.base + 8L + (PUnsafe.getLong(this.addr + this.base) << 3L) + 8L;
    }

    @Override
    public long alloc(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void free(long base) {
        throw new UnsupportedOperationException();
    }

    public Spliterator.OfLong spliterator() {
        MemoryHelper.prefetch(this.addr, this.actualSize0());

        @AllArgsConstructor
        class BitSetSpliterator implements java.util.Spliterator.OfLong {
            protected final long addr;
            protected long index;
            protected final long end;

            @Override
            public OfLong trySplit() {
                long index = this.index;
                long sizeWords = (this.end - index) >>> 6L;
                if (sizeWords >= 256L) {
                    long middle = (index + this.end) >>> 1L;
                    this.index = middle;
                    return new BitSetSpliterator(this.addr, index, middle);
                }
                return null;
            }

            @Override
            public boolean tryAdvance(@NonNull LongConsumer action) {
                while (true) { //dumb and slow implementation that will likely almost never be used
                    long index = this.index;
                    if (index < this.end) {
                        this.index = index + 1L;
                        if ((PUnsafe.getLong(this.addr + (index >>> 6L << 3L)) & (1L << (index & 0x3FL))) != 0L) {
                            action.accept(index);
                            return true;
                        }
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public void forEachRemaining(@NonNull LongConsumer action) {
                while ((this.index & 0x3FL) != 0L) {
                    if (!this.tryAdvance(action)) {
                        return;
                    }
                }

                for (long word = this.index >>> 6L, endWord = this.end >>> 6L; word < endWord; word++, this.index += 64L) {
                    long v = PUnsafe.getLong(this.addr + (word << 3L));
                    if (v != 0L) {
                        for (long shift = 0L; shift < 64L; shift++) {
                            if ((v & (1L << shift)) != 0L) {
                                action.accept((word << 6L) + shift);
                            }
                        }
                    }
                }

                while (this.index < this.end) {
                    if (!this.tryAdvance(action)) {
                        return;
                    }
                }
            }

            @Override
            public long estimateSize() {
                long count = 0L;
                for (long word = this.index >>> 6L, endWord = this.end >>> 6L; word < endWord; word++) {
                    count += Long.bitCount(PUnsafe.getLong(this.addr + (word << 3L)));
                }
                return count;
            }

            @Override
            public int characteristics() {
                return Spliterator.ORDERED | Spliterator.DISTINCT;
            }
        }

        long beginAddr = this.addr + this.base + 8L;
        return new BitSetSpliterator(beginAddr, 0L, (PUnsafe.getLongVolatile(null, this.addr + this.base) + 8L) << 6L);
    }
}
