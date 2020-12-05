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

package net.daporkchop.tpposmtilegen.input.read;

import io.netty.util.internal.PlatformDependent;
import lombok.NonNull;
import net.daporkchop.lib.binary.netty.buf.WrappedUnpooledUnsafeDirectByteBuf;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.input.DataProcessor;
import net.daporkchop.tpposmtilegen.input.InputParser;
import net.daporkchop.tpposmtilegen.input.InputReader;
import net.daporkchop.tpposmtilegen.util.RefCountedMappedByteBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class MemoryMappedSegmentedReader implements InputReader {
    @Override
    public <D> void read(@NonNull File file, @NonNull InputParser<D> parser, @NonNull DataProcessor<D> processor) throws IOException {
        //map the entire file into memory
        RefCountedMappedByteBuffer buffer;
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            buffer = new RefCountedMappedByteBuffer(channel, FileChannel.MapMode.READ_ONLY, 0L, channel.size());
        }

        try {
            long addr = buffer.addr();
            long size = buffer.size();
            long start = 0L;
            for (long i = 0L; i < size; i++) {
                if (PUnsafe.getByte(addr + i) == '\n') {
                    if (start < i) {
                        parser.parse(new MemoryMappedSlice(buffer, start, toInt(i - start)), processor);
                    }
                    start = i + 1L;
                }
            }
            if (start < size) {
                parser.parse(new MemoryMappedSlice(buffer, start, toInt(size - start)), processor);
            }
        } finally {
            buffer.release();
        }
    }

    private static final class MemoryMappedSlice extends WrappedUnpooledUnsafeDirectByteBuf {
        protected final RefCountedMappedByteBuffer buffer;

        public MemoryMappedSlice(@NonNull RefCountedMappedByteBuffer buffer, long offset, int size) {
            super(PlatformDependent.directBuffer(buffer.addr() + offset, size), size);

            this.buffer = buffer.retain();
        }

        @Override
        protected void deallocate() {
            this.buffer.release();
        }
    }
}
