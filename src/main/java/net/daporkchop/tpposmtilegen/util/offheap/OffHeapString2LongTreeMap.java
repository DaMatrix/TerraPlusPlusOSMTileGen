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
import net.daporkchop.lib.primitive.lambda.LongLongConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.cstring;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.dynamic.SequentialDynamicAllocator;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author DaPorkchop_
 */
public class OffHeapString2LongTreeMap extends SequentialDynamicAllocator {
    /*
     * struct Node {
     *   Node* high;
     *   Node* low;
     *   long value;
     *   char key[];
     * };
     */

    protected static final long HIGH_OFFSET = 0L;
    protected static final long LOW_OFFSET = HIGH_OFFSET + 8L;
    protected static final long VALUE_OFFSET = LOW_OFFSET + 8L;
    protected static final long KEY_OFFSET = VALUE_OFFSET + 8L;

    //offset of root node pointer
    protected final long root = super.headerSize();

    public OffHeapString2LongTreeMap(@NonNull Path path) throws IOException {
        super(path);
    }

    public void put(long key, long value) {
        long currPtr = this.root;
        long node;
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            while ((node = PUnsafe.getLong(addr + currPtr)) != 0L) { //root node is set, traverse it
                int d = cstring.strcmp(key, addr + node + KEY_OFFSET);
                if (d == 0) {
                    PUnsafe.putLong(addr + node + VALUE_OFFSET, value);
                    return;
                }
                currPtr = node + (d > 0 ? HIGH_OFFSET : LOW_OFFSET);
            }
        }

        //if we get this far, we need to allocate a new node
        long keySize = cstring.strlen(key);

        node = this.alloc(KEY_OFFSET + keySize);
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            PUnsafe.putLong(addr + node + HIGH_OFFSET, 0L);
            PUnsafe.putLong(addr + node + LOW_OFFSET, 0L);
            PUnsafe.putLong(addr + node + VALUE_OFFSET, value);
            PUnsafe.copyMemory(key, addr + node + KEY_OFFSET, keySize);

            PUnsafe.putLong(addr + currPtr, node);
        }
    }

    public void increment(long key) {
        long currPtr = this.root;
        long node;
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            while ((node = PUnsafe.getLong(addr + currPtr)) != 0L) { //root node is set, traverse it
                int d = cstring.strcmp(key, addr + node + KEY_OFFSET);
                if (d == 0) {
                    PUnsafe.putLong(addr + node + VALUE_OFFSET, PUnsafe.getLong(addr + node + VALUE_OFFSET) + 1L);
                    return;
                }
                currPtr = node + (d > 0 ? HIGH_OFFSET : LOW_OFFSET);
            }
        }

        //if we get this far, we need to allocate a new node
        long keySize = cstring.strlen(key);

        node = this.alloc(KEY_OFFSET + keySize + 1);
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            PUnsafe.putLong(addr + node + HIGH_OFFSET, 0L);
            PUnsafe.putLong(addr + node + LOW_OFFSET, 0L);
            PUnsafe.putLong(addr + node + VALUE_OFFSET, 1L);
            PUnsafe.copyMemory(key, addr + node + KEY_OFFSET, keySize);
            PUnsafe.putByte(addr + node + KEY_OFFSET + keySize, (byte) 0);

            PUnsafe.putLong(addr + currPtr, node);
        }
    }

    public long get(long key) {
        long currPtr = this.root;
        long node;
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            while ((node = PUnsafe.getLong(addr + currPtr)) != 0L) { //root node is set, traverse it
                int d = cstring.strcmp(key, addr + node + KEY_OFFSET);
                if (d == 0) {
                    return PUnsafe.getLong(addr + node + VALUE_OFFSET);
                }
                currPtr = node + (d > 0 ? HIGH_OFFSET : LOW_OFFSET);
            }
        }
        return -1L;
    }

    public void forEach(@NonNull LongLongConsumer callback) {
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            long node = PUnsafe.getLong(addr + this.root);
            if (node != 0L) {
                this.forEach0(addr, node, callback);
            }
        }
    }

    protected void forEach0(long addr, long node, LongLongConsumer callback) {
        long next;
        if ((next = PUnsafe.getLong(addr + node + LOW_OFFSET)) != 0L) {
            this.forEach0(addr, next, callback);
        }
        callback.accept(addr + node + KEY_OFFSET, PUnsafe.getLong(addr + node + VALUE_OFFSET));
        if ((next = PUnsafe.getLong(addr + node + HIGH_OFFSET)) != 0L) {
            this.forEach0(addr, next, callback);
        }
    }

    @Override
    protected long headerSize() {
        return super.headerSize() + 8L;
    }

    @Override
    protected void initHeaders(long addr) {
        super.initHeaders(addr);
    }
}
