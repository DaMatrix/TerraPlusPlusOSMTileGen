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
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
class ImmediateMetablockSequence implements ISquashfsBuilder {
    protected final SquashfsBuilder parent;

    protected final Path file;
    protected final FileChannel channel;

    protected final Compression compression;

    protected final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer();

    @Getter
    private int bytesWritten = 0;
    @Getter
    private int blocksWritten = 0;

    public ImmediateMetablockSequence(@NonNull Compression compression, @NonNull Path file, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;

        this.file = file;
        this.channel = FileChannel.open(this.file, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        this.compression = compression;
    }

    public int inBlockOffset() {
        return this.buffer.readableBytes();
    }

    public void write(@NonNull ByteBuf buffer) throws IOException {
        this.buffer.writeBytes(buffer);

        this.flush();
    }

    private void flush() throws IOException {
        if (this.buffer.readableBytes() >= METABLOCK_MAX_SIZE) { //flush as many pending metablocks as possible
            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf dst = recycler.get();
            try {
                do {
                    writeCompressedMetablock(this.compression, this.buffer.readSlice(METABLOCK_MAX_SIZE), dst);
                    this.blocksWritten++;
                } while (this.buffer.readableBytes() >= METABLOCK_MAX_SIZE);
                this.buffer.discardSomeReadBytes();

                this.bytesWritten += dst.readableBytes();
                writeFully(this.channel, dst);
            } finally {
                recycler.release(dst);
            }
        }
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        if (this.buffer.isReadable()) {
            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf dst = recycler.get();
            try {
                do {
                    int blockSize = min(this.buffer.readableBytes(), METABLOCK_MAX_SIZE);
                    writeCompressedMetablock(this.compression, this.buffer.readSlice(blockSize), dst);
                    this.blocksWritten++;
                } while (this.buffer.isReadable());

                this.bytesWritten += dst.readableBytes();
                writeFully(this.channel, dst);
            } finally {
                recycler.release(dst);
            }
        }
        this.buffer.release();
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        transferFully(this.channel, channel);
    }

    @Override
    public void close() throws IOException {
        this.channel.close();

        Files.delete(this.file);
    }
}
