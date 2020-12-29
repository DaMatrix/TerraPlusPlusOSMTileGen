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

package net.daporkchop.tpposmtilegen.util.offheap;

import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.sparse.AbstractSparseAllocator;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An off-heap, memory-mapped bitset which supports 64-bit indexing.
 *
 * @author DaPorkchop_
 */
public class OffHeapBitSet extends AbstractSparseAllocator {
    private final long base = super.headerSize();

    public OffHeapBitSet(@NonNull Path path, long size) throws IOException {
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

    public void set(long index) {
        long wordIndex = index >>> 6L;
        this.ensureCapacity(wordIndex);

        long wordAddr = this.addr + this.base + 8L + (wordIndex << 3L);
        long val;
        do {
            val = PUnsafe.getLongVolatile(null, wordAddr);
        } while (!PUnsafe.compareAndSwapLong(null, wordAddr, val, val | (1L << (index & 0x3FL))));
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
        long currSize = PUnsafe.getLong(addr + this.base);
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
}
