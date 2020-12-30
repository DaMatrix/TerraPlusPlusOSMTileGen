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

package net.daporkchop.tpposmtilegen.util.mmap;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PCleaner;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
public abstract class SparseMemoryMap implements AutoCloseable {
    protected final FileChannel channel;

    protected final long addr;
    protected final long size;

    @Getter(AccessLevel.NONE)
    protected final PCleaner cleaner;

    public SparseMemoryMap(@NonNull Path path, long size) throws IOException {
        this.size = positive(size, "size");

        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE, StandardOpenOption.CREATE);
        if (this.channel.size() < size) { //grow file if needed
            MemoryMap.truncate0(this.channel, size);
        }

        this.addr = MemoryMap.map0(this.channel, MemoryMap.IMODE_RW, 0L, size);

        this.cleaner = PCleaner.cleaner(this, new MemoryMap.Unmapper(this.addr, this.size));
    }

    @Override
    public void close() throws IOException {
        long actualSize = this.actualSize0();

        this.cleaner.clean();

        if (actualSize != this.size) {
            MemoryMap.truncate0(this.channel, actualSize);
        }
        this.channel.close();
    }

    protected long actualSize0() {
        return this.size;
    }
}
