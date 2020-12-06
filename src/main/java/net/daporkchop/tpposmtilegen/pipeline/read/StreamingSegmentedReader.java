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

package net.daporkchop.tpposmtilegen.pipeline.read;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataIn;
import net.daporkchop.tpposmtilegen.pipeline.FilterPipelineStep;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.util.Util;

import java.io.File;
import java.io.IOException;

/**
 * Reads input data segmented by newlines.
 *
 * @author DaPorkchop_
 */
public class StreamingSegmentedReader extends FilterPipelineStep<File, ByteBuf> {
    private static final int BLOCK_SIZE = 1 << 16;

    public StreamingSegmentedReader(PipelineStep<ByteBuf> next) {
        super(next);
    }

    @Override
    public void accept(@NonNull File file) throws IOException {
        ByteBuf readBuffer = ByteBufAllocator.DEFAULT.buffer(BLOCK_SIZE);
        try (DataIn in = Util.readerFor(file)) {
            while (in.read(readBuffer, BLOCK_SIZE) >= 0) {
                readBuffer = this.processBlock(readBuffer);
            }

            if (readBuffer.resetReaderIndex().isReadable()) { //pass rest of data to parser
                this.next.accept(readBuffer.retain());
            }
        } finally {
            readBuffer.release();
        }
    }

    private ByteBuf processBlock(ByteBuf readBuffer) throws IOException {
        int start = 0; //index of the first char of the current line
        int writerIndex = readBuffer.writerIndex();
        int limit = writerIndex - 1; //the index to read up to

        for (int i = readBuffer.readerIndex(); i < limit; i++) {
            if (readBuffer.getByte(i) == '\n') {
                this.next.accept(readBuffer.retainedSlice(start, i - start));
                start = i + 1;
            }
        }

        if (start != 0) { //we read at least one line, discard buffer and copy remaining data to a new one
            int length = writerIndex - start;
            ByteBuf newBuffer = ByteBufAllocator.DEFAULT.buffer(length + BLOCK_SIZE);
            readBuffer.getBytes(start, newBuffer, length).release();
            readBuffer = newBuffer;
        } else {
            readBuffer.readerIndex(limit);
        }
        return readBuffer;
    }
}
