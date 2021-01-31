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
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.lang.Math.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
class MetablockSequence extends CompressedBlockSequence {
    protected final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer();

    @Getter
    protected final String name;

    public MetablockSequence(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent, @NonNull String name) throws IOException {
        super(compression, root, parent);

        this.name = name;
    }

    @Override
    protected int blockSize0() {
        return METABLOCK_MAX_SIZE;
    }

    @Override
    protected boolean compress(ByteBuf readBuffer, ByteBuf writeBuffer) throws IOException {
        int origSize = readBuffer.readableBytes();
        this.compression.compress(readBuffer, writeBuffer.writeShortLE(-1));

        if (writeBuffer.readableBytes() >= origSize) {
            writeBuffer.writerIndex(0).writeShortLE(this.toReferenceSize(true, origSize)).writeBytes(readBuffer.readerIndex(0));
            return true;
        }

        writeBuffer.setShortLE(0, this.toReferenceSize(false, writeBuffer.readableBytes() - 2));
        return false;
    }

    @Override
    protected int toReferenceSize(boolean uncompressed, int compressedSize) {
        return uncompressed ? compressedSize | METABLOCK_HEADER_UNCOMPRESSED_FLAG : compressedSize;
    }

    public void write(@NonNull ByteBuf buffer) throws IOException {
        this.buffer.writeBytes(buffer);
        if (this.buffer.readableBytes() >= METABLOCK_MAX_SIZE) { //flush as many complete metablocks as possible
            do {
                this.putBlock(this.buffer.readSlice(METABLOCK_MAX_SIZE));
            } while (this.buffer.readableBytes() >= METABLOCK_MAX_SIZE);
            this.buffer.discardSomeReadBytes();
        }
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        if (this.buffer.isReadable()) { //flush all remaining buffered data
            do {
                int blockSize = min(this.buffer.readableBytes(), METABLOCK_MAX_SIZE);
                this.putBlock(this.buffer.readSlice(blockSize));
            } while (this.buffer.isReadable());
        }
        this.buffer.release();

        super.finish(channel, superblock);
    }
}
