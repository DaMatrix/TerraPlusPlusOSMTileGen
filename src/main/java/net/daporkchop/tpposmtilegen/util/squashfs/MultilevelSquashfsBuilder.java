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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.daporkchop.tpposmtilegen.util.Utils.*;

/**
 * @author DaPorkchop_
 */
public abstract class MultilevelSquashfsBuilder implements ISquashfsBuilder {
    protected final Path root;

    protected final SquashfsBuilder parent;
    protected final MetablockSequence writer;

    protected final IntList offsets = new IntArrayList();
    @Getter
    protected long count = 0L;

    public MultilevelSquashfsBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;

        this.root = Files.createDirectories(root);

        this.writer = new MetablockSequence(compression, root.resolve("metablocks")) {
            @Override
            protected void writeBlockCallback(int offset, int originalSize, int compressedSize) throws IOException {
                MultilevelSquashfsBuilder.this.offsets.add(offset);
            }
        };
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        this.writer.finish(channel, superblock);
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        long baseOffset = channel.position();

        this.writer.transferTo(channel, superblock);

        this.applyStartOffset(superblock, channel.position());

        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf dst = recycler.get();
        try {
            for (int offset : this.offsets) {
                dst.writeLongLE(baseOffset + offset);
            }

            writeFully(channel, dst);
        } finally {
            recycler.release(dst);
        }
    }

    protected abstract void applyStartOffset(@NonNull Superblock superblock, long startOffset);

    @Override
    public void close() throws IOException {
        this.writer.close();

        Files.delete(this.root);
    }
}
