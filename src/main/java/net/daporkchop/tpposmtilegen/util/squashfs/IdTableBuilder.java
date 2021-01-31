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

package net.daporkchop.tpposmtilegen.util.squashfs;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.lang.Math.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
final class IdTableBuilder implements ISquashfsBuilder {
    protected final SquashfsBuilder parent;
    protected final MetablockWriter writer;

    protected final LongList offsets = new LongArrayList();
    @Getter
    protected int count = 0;

    public IdTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;
        this.writer = new MetablockWriter(compression, root) {
            @Override
            protected void writeBlockCallback(int offset, int originalSize, int compressedSize) throws IOException {
                IdTableBuilder.this.offsets.add(offset);
            }
        };
    }

    public void putId(int id) throws IOException {
        this.count++;
        this.writer.buffer().writeIntLE(id);
        this.writer.flush();
    }

    @Override
    public void finish() throws IOException {
        this.writer.finish();
    }

    @Override
    public void transferTo(@NonNull FileChannel channel) throws IOException {
        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf dst = recycler.get();
        try {
            long baseOffset = channel.position() + ((long) this.offsets.size() << 3L);
            for (long offset : this.offsets) {
                dst.writeLongLE(baseOffset + offset);
            }

            writeFully(channel, dst);
        } finally {
            recycler.release(dst);
        }

        this.writer.transferTo(channel);
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
