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

package net.daporkchop.tpposmtilegen.util.mmap;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.refcount.AbstractRefCounted;
import net.daporkchop.lib.common.util.exception.AlreadyReleasedException;
import net.daporkchop.tpposmtilegen.util.mmap.growfunc.DefaultGrowFunction;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.LongUnaryOperator;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
public class DynamicMemoryMap extends AbstractRefCounted {
    protected final LongUnaryOperator growFunction;
    protected final FileChannel channel;

    protected volatile MemoryMap buffer;
    protected volatile long size;

    public DynamicMemoryMap(@NonNull Path path) throws IOException {
        this(path, DefaultGrowFunction.INSTANCE);
    }

    public DynamicMemoryMap(@NonNull Path path, @NonNull LongUnaryOperator growFunction) throws IOException {
        this.growFunction = growFunction;
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

        this.buffer = new MemoryMap(this.channel, FileChannel.MapMode.READ_WRITE, 0L, this.size = this.channel.size());
    }

    public MemoryMap ensureCapacity(long capacity) {
        if (notNegative(capacity, "capacity") > this.size) {
            synchronized (this) {
                if (capacity > this.size) { //check again, in case the capacity was increased while waiting to acquire the lock
                    long newSize = this.size;
                    do {
                        newSize = this.growFunction.applyAsLong(newSize);
                    } while (newSize < capacity); //continually apply the grow function until we reach maximum capacity

                    MemoryMap.truncate0(this.channel, newSize);

                    //replace the old map with the new one
                    this.buffer = new MemoryMap(this.channel, FileChannel.MapMode.READ_WRITE, 0L, this.size = newSize);
                    System.gc(); //remind the garbage collector to attempt a full GC to release mappings that are no longer used
                }
            }
        }
        return this.buffer;
    }

    @Override
    public DynamicMemoryMap retain() throws AlreadyReleasedException {
        super.retain();
        return this;
    }

    @Override
    protected void doRelease() {
        try {
            this.channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
