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
class MetablockWriter implements ISquashfsBuilder {
    protected final Path root;
    protected final FileChannel channel;

    protected final Compression compression;
    @Getter
    protected final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer(METABLOCK_MAX_SIZE << 1);

    @Getter
    protected int bytesWritten = 0;
    @Getter
    protected int blocksWritten = 0;

    public MetablockWriter(@NonNull Compression compression, @NonNull Path root) throws IOException {
        this.compression = compression;
        this.channel = FileChannel.open(this.root = root, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    public void write(@NonNull ByteBuf buffer) throws IOException {
        this.buffer.writeBytes(buffer);

        this.flush();
    }

    public void flush() throws IOException {
        if (this.buffer.readableBytes() >= METABLOCK_MAX_SIZE) { //flush as many pending metablocks as possible
            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf dst = recycler.get();
            try {
                do {
                    int startWriterIndex = dst.writerIndex();
                    int offset = this.bytesWritten + dst.readableBytes();
                    writeCompressedMetablock(this.compression, this.buffer.readSlice(METABLOCK_MAX_SIZE), dst);
                    this.writeBlockCallback(offset, METABLOCK_MAX_SIZE, dst.writerIndex() - startWriterIndex);
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
    public void finish(@NonNull Superblock superblock) throws IOException {
        if (this.buffer.isReadable()) {
            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf dst = recycler.get();
            try {
                do {
                    int blockSize = min(this.buffer.readableBytes(), METABLOCK_MAX_SIZE);
                    int startWriterIndex = dst.writerIndex();
                    int offset = this.bytesWritten + dst.readableBytes();
                    writeCompressedMetablock(this.compression, this.buffer.readSlice(blockSize), dst);
                    this.writeBlockCallback(offset, blockSize, dst.writerIndex() - startWriterIndex);
                } while (this.buffer.isReadable());

                this.bytesWritten += dst.readableBytes();
                writeFully(this.channel, dst);
            } finally {
                recycler.release(dst);
            }
        }
        this.buffer.release();
    }

    protected void writeBlockCallback(int offset, int originalSize, int compressedSize) throws IOException {
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        checkState(this.buffer.refCnt() == 0, "not finished!");

        long size = this.channel.size();
        for (long pos = 0L; pos < size; pos += this.channel.transferTo(pos, size - pos, channel)) {
        }
    }

    @Override
    public void close() throws IOException {
        this.channel.close();

        Files.delete(this.root);
    }
}
