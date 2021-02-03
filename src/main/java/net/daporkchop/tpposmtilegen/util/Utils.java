/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2021 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Utils {
    public final Ref<SimpleRecycler<ByteBuf>> IO_BUFFER_RECYCLER = ThreadRef.late(() -> new SimpleRecycler<ByteBuf>() {
        @Override
        protected ByteBuf newInstance0() {
            return Unpooled.directBuffer();
        }

        @Override
        protected void reset0(@NonNull ByteBuf value) {
            value.clear();
        }

        @Override
        protected boolean hasCapacity() {
            return this.size() < 16;
        }
    });

    public void writeFully(@NonNull FileChannel dst, @NonNull ByteBuf src) throws IOException {
        while (src.isReadable()) {
            src.readBytes(dst, src.readableBytes());
        }
    }

    public void writeFully(@NonNull FileChannel dst, long pos, @NonNull ByteBuf src) throws IOException {
        while (src.isReadable()) {
            pos += src.readBytes(dst, pos, src.readableBytes());
        }
    }

    public void readFully(@NonNull FileChannel src, @NonNull ByteBuf dst, int count) throws IOException {
        dst.ensureWritable(count);
        for (int i = 0; i < count; i += dst.writeBytes(src, count - i)) {
        }
    }

    public void readFully(@NonNull FileChannel src, long pos, @NonNull ByteBuf dst, int count) throws IOException {
        dst.ensureWritable(count);
        for (int i = 0; i < count; i += dst.writeBytes(src, pos + i, count - i)) {
        }
    }

    public void transferFully(@NonNull FileChannel src, @NonNull FileChannel dst) throws IOException {
        for (long pos = 0L, count = src.position(); pos < count; pos += src.transferTo(pos, count - pos, dst)) {
        }
    }

    public void padTo4k(@NonNull FileChannel channel) throws IOException {
        long capacity = channel.position();
        if ((capacity & 0xFFFL) != 0L) {
            writeFully(channel, Unpooled.wrappedBuffer(new byte[toInt(4096L - (capacity & 0xFFFL))]));
        }
    }

    public int u16(int val) {
        checkArg((val & 0xFFFF) == val);
        return val;
    }

    public int u16(long val) {
        checkArg((val & 0xFFFFL) == val);
        return (int) val;
    }

    public int u32(long val) {
        checkArg((val & 0xFFFFFFFFL) == val);
        return (int) val;
    }

    public long u64(long val) {
        checkArg(val >= 0L);
        return val;
    }
}
