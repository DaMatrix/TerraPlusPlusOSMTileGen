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
import io.netty.buffer.UnpooledByteBufAllocator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.lang.Math.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
final class BlockTableBuilder implements ISquashfsBuilder {
    protected final SquashfsBuilder parent;

    protected final Path file;
    protected final FileChannel channel;

    protected final Compression compression;
    protected final ByteBuf buffer;

    protected long writtenBytes = 0L;

    public BlockTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;

        this.file = root;
        this.channel = FileChannel.open(this.file, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        this.compression = compression;
        this.buffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer();
    }

    public int[] write(@NonNull ByteBuf data) throws IOException {
        IntList list = new IntArrayList();

        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf dst = recycler.get();
        try {
            while (data.isReadable()) {
                int blockSize = min(data.readableBytes(), 1 << this.parent.blockLog);
                ByteBuf block = data.readSlice(blockSize);
                this.compression.compress(block, dst);

                if (dst.readableBytes() >= blockSize) { //write uncompressed
                    block.readerIndex(0);
                    writeFully(this.channel, block);
                    list.add(blockSize | DATA_BLOCK_UNCOMPRESSED_FLAG);
                } else {
                    list.add(dst.readableBytes());
                    writeFully(this.channel, dst);
                }
            }
        } finally {
            recycler.release(dst);
        }

        return list.toIntArray();
    }

    @Override
    public void finish(@NonNull Superblock superblock) throws IOException {
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        long size = this.channel.size();
        for (long pos = 0L; pos < size; pos += this.channel.transferTo(pos, size - pos, channel)) {
        }
    }

    @Override
    public void close() throws IOException {
        this.buffer.release();

        this.channel.close();

        Files.delete(this.file);
    }
}
