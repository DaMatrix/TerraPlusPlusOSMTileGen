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

import io.netty.buffer.Unpooled;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class OffHeapAtomicLong implements AutoCloseable {
    protected final MemoryMap map;
    protected final long addr;

    public OffHeapAtomicLong(@NonNull Path path, long defaultValue) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            if (channel.size() != 8L) { //reset value to default
                channel.truncate(0L);
                Unpooled.wrappedBuffer(new byte[8]).clear().writeLongLE(defaultValue).readBytes(channel, 8);
                checkState(channel.size() == 8L);
            }
            this.map = new MemoryMap(channel, FileChannel.MapMode.READ_WRITE, 0L, 8L);
        }
        this.addr = this.map.addr();
    }

    public long get() {
        return PUnsafe.getLongVolatile(null, this.addr);
    }

    public void set(long value) {
        PUnsafe.putLongVolatile(null, this.addr, value);
    }

    public long max(long value) {
        long curr;
        do {
            curr = PUnsafe.getLongVolatile(null, this.addr);
        } while (value > curr && !PUnsafe.compareAndSwapLong(null, this.addr, curr, value));
        return Math.max(curr, value);
    }

    @Override
    public void close() throws IOException {
        this.map.close();
    }
}
