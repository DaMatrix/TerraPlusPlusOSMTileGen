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

package net.daporkchop.tpposmtilegen.util.mmap.alloc.sparse;

import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.Memory;
import net.daporkchop.tpposmtilegen.util.mmap.SparseMemoryMap;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.Allocator;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author DaPorkchop_
 */
public abstract class AbstractSparseAllocator extends SparseMemoryMap implements Allocator {
    private static final long MAGIC = 0xDEADBEEF_F000BAAAL;

    public AbstractSparseAllocator(@NonNull Path path, long size) throws IOException {
        super(path, size);

        if (PUnsafe.getLong(this.addr) != MAGIC) { //file hasn't been initialized yet
            this.initHeaders();
        }
    }

    protected long headerSize() {
        return 8L;
    }

    protected void initHeaders() {
        Memory.madvise(this.addr, this.size, Memory.Usage.MADV_REMOVE);
        PUnsafe.setMemory(this.addr, this.headerSize(), (byte) 0);
        PUnsafe.putLong(this.addr, MAGIC);
    }
}
