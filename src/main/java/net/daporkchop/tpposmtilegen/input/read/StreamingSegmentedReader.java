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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.binary.stream.DataIn;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.tpposmtilegen.util.Util;

import java.io.File;
import java.io.IOException;

/**
 * Reads input data segmented by newlines.
 *
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public class StreamingSegmentedReader implements IOConsumer<File> {
    private static final int BLOCK_SIZE = 1 << 16;

    @NonNull
    protected final IOConsumer<ByteBuf> next;

    @Override
    public void acceptThrowing(@NonNull File file) throws IOException {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(BLOCK_SIZE);
        try (DataIn in = Util.readerFor(file)) {
            while (in.read(buf, BLOCK_SIZE) >= 0) {
                int mark = buf.readerIndex();
                buf.resetReaderIndex();
                while (buf.isReadable(2)) {
                    if (buf.readByte() == '\n') {
                        int end = buf.readerIndex();
                        int start = mark;
                        int len = end - start - 1;
                        mark = end;
                        ByteBuf blob = ByteBufAllocator.DEFAULT.buffer(len, len);
                        buf.getBytes(start, blob);
                        this.next.acceptThrowing(blob);
                    }
                }
                buf.markReaderIndex().readerIndex(mark);
                buf.discardSomeReadBytes();
            }

            if (buf.resetReaderIndex().isReadable()) { //pass rest of data to parser
                this.next.acceptThrowing(buf.retain());
            }
        } finally {
            buf.release();
        }
    }
}
