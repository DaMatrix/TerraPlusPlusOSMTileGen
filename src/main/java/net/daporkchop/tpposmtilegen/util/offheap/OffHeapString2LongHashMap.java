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
import net.daporkchop.lib.common.math.BinMath;
import net.daporkchop.lib.primitive.lambda.LongLongConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.cstring;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.SequentialAllocator;

import java.io.IOException;
import java.nio.file.Path;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class OffHeapString2LongHashMap extends SequentialAllocator {
    protected static final long TABLE_NODE_OFFSET = 0L;
    protected static final long TABLE_LOCK_OFFSET = TABLE_NODE_OFFSET + 8L;
    protected static final long TABLE_SIZE = TABLE_LOCK_OFFSET + OffHeapLock.SIZE;

    protected static final long NODE_NEXT_OFFSET = 0L;
    protected static final long NODE_VALUE_OFFSET = NODE_NEXT_OFFSET + 8L;
    protected static final long NODE_KEY_OFFSET = NODE_VALUE_OFFSET + 8L;

    //offset of info block
    protected final long root = super.headerSize();
    //offset of table
    protected final long table;

    protected final long tableSize;

    public OffHeapString2LongHashMap(@NonNull Path path, long tableSize) throws IOException {
        super(path);

        tableSize = BinMath.roundToNearestPowerOf2(positive(tableSize, "tableSize"));

        try (MemoryMap map = this.buffer) {
            long addr = map.addr();
            if (PUnsafe.getLong(addr + this.root) == 0L) {
                PUnsafe.putLong(addr + this.root, this.tableSize = tableSize);
                PUnsafe.putLong(addr + this.root + 8L, this.table = this.alloc(tableSize * TABLE_SIZE));

                try (MemoryMap map2 = this.buffer) { //file may have grown
                    addr = map2.addr();
                    //initialize table
                    for (long l = 0L; l < this.tableSize; l++) {
                        long bucket = this.table + l * TABLE_SIZE;
                        PUnsafe.putLong(addr + bucket + TABLE_NODE_OFFSET, 0L);
                        OffHeapLock.init(addr + bucket + TABLE_LOCK_OFFSET);
                    }
                }
            } else {
                this.tableSize = PUnsafe.getLong(addr + this.root);
                this.table = PUnsafe.getLong(addr + this.root + 8L);
            }
        }
    }

    public void increment(long key) {
        long hash = cstring.hash(key);
        long bucket = this.table + (hash & (this.tableSize - 1L)) * TABLE_SIZE;
        long currPtr;

        MemoryMap mmap = this.buffer;
        long addr = mmap.addr();
        try {
            //lock bucket
            OffHeapLock.lock(addr + bucket + TABLE_LOCK_OFFSET);

            currPtr = bucket + TABLE_NODE_OFFSET;
            long node;
            while ((node = PUnsafe.getLong(addr + currPtr)) != 0L) {
                if (cstring.strcmp(key, addr + node + NODE_KEY_OFFSET) == 0) { //found matching node! increment value and exit
                    PUnsafe.putLong(addr + node + NODE_VALUE_OFFSET, PUnsafe.getLong(addr + node + NODE_VALUE_OFFSET) + 1L);
                    PUnsafe.storeFence();
                    OffHeapLock.unlock(addr + bucket + TABLE_LOCK_OFFSET);
                    return;
                }

                currPtr = node + NODE_NEXT_OFFSET;
            }
        } finally {
            PUnsafe.loadFence();
            PUnsafe.storeFence();
            mmap.close();
            mmap = null;
        }

        //if we get this far, we need to allocate a new node
        long keySize = cstring.strlen(key);
        long node = this.alloc(NODE_KEY_OFFSET + keySize + 1);

        mmap = this.buffer;
        addr = mmap.addr();
        try {
            PUnsafe.putLong(addr + node + NODE_NEXT_OFFSET, 0L);
            PUnsafe.putLong(addr + node + NODE_VALUE_OFFSET, 1L);
            PUnsafe.copyMemory(key, addr + node + NODE_KEY_OFFSET, keySize);
            PUnsafe.putByte(addr + node + NODE_KEY_OFFSET + keySize, (byte) 0);

            PUnsafe.putLong(addr + currPtr, node);

            OffHeapLock.unlock(addr + bucket + TABLE_LOCK_OFFSET);
        } finally {
            PUnsafe.loadFence();
            PUnsafe.storeFence();
            mmap.close();
            mmap = null;
        }
    }

    public void forEach(@NonNull LongLongConsumer callback) {
        try (MemoryMap mmap = this.buffer) {
            long addr = mmap.addr();
            for (long l = 0L; l < this.tableSize; l++) {
                long bucket = this.table + l * TABLE_SIZE;

                //no locking required, we're only reading!
                //...this isn't safe though, it'll break if the table is resized while iterating.
                for (long node = PUnsafe.getLong(addr + bucket + TABLE_NODE_OFFSET); node != 0L; node = PUnsafe.getLong(addr + node + NODE_NEXT_OFFSET)) {
                    callback.accept(addr + node + NODE_KEY_OFFSET, PUnsafe.getLong(addr + node + NODE_VALUE_OFFSET));
                }
            }
        }
    }

    @Override
    protected long headerSize() {
        return super.headerSize() + 16L;
    }

    @Override
    protected void initHeaders(long addr) {
        super.initHeaders(addr);
    }
}
