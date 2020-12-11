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

package net.daporkchop.tpposmtilegen.util.mmap.alloc;

import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;
import net.daporkchop.tpposmtilegen.util.mmap.RefCountedMemoryMap;
import net.daporkchop.tpposmtilegen.util.mmap.DynamicMemoryMap;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * A simple allocator which works by incrementing a counter to keep track of the current file head, and doesn't support releasing memory.
 *
 * @author DaPorkchop_
 */
public class SequentialAllocator implements Allocator {
    protected final DynamicMemoryMap file;

    public SequentialAllocator(@NonNull DynamicMemoryMap file) {
        this.file = file;

        if (this.file.size() < 8L) { //file hasn't been initialized yet
            this.file.ensureCapacity(8L);
            try (MemoryMap buffer = this.file.buffer()) { //next offset should be at 8 (not 0, there's a header lol)
                PUnsafe.putLong(buffer.addr(), 8L);
            }
        }
    }

    @Override
    public long alloc(long size) {
        positive(size, "size");
        try (MemoryMap buffer = this.file.buffer()) {
            return PUnsafe.getAndAddLong(null, buffer.addr(), size);
        }
    }

    @Override
    public void free(long base) {
        //no-op
    }
}
