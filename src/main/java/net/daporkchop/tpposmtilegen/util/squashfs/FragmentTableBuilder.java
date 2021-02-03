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
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;

/**
 * @author DaPorkchop_
 */
final class FragmentTableBuilder extends MultilevelSquashfsBuilder {
    private final ByteBuf dataBuffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer();

    protected final Path indexFile;
    protected final FileChannel indexChannel;

    protected int blockIdAllocator = 0;

    public FragmentTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        super(compression, root, parent);

        this.indexChannel = FileChannel.open(this.indexFile = root.resolve("index"), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    @Override
    protected String name() {
        return "fragment table";
    }

    public FragmentBlockEntry appendFragmentData(@NonNull ByteBuf data) throws IOException {
        int readableBytes = data.readableBytes();
        int blockSize = 1 << this.parent.blockLog;
        checkArg(readableBytes < blockSize, "fragment data too big: %d > %d", readableBytes, blockSize);

        if (this.dataBuffer.readableBytes() + readableBytes >= blockSize) { //flush block
            this.flush(false);
        }

        FragmentBlockEntry entry = new FragmentBlockEntry(this.blockIdAllocator, this.dataBuffer.readableBytes());
        this.dataBuffer.writeBytes(data);
        return entry;
    }

    private void flush(boolean sync) throws IOException {
        long id = this.parent.blockTable.writeBlocks(this.dataBuffer, 1, sync);
        this.dataBuffer.clear();

        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf dst = recycler.get();
        try {
            dst.writeLongLE(id);
            writeFully(this.indexChannel, dst);
        } finally {
            recycler.release(dst);
        }

        this.blockIdAllocator++;
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        if (this.dataBuffer.isReadable()) {
            this.flush(true);
        }
        this.dataBuffer.release();

        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf dst = recycler.get();
        try {
            for (int i = 0; i < this.blockIdAllocator; i++) {
                readFully(this.indexChannel, i * 8L, dst, 8);
                DatablockReference reference = this.parent.blockTable.getReference(dst.readLongLE());
                dst.clear().writeLongLE(reference.blockStart()).writeIntLE(reference.size()).writeIntLE(0);
                this.writer.write(dst);
                dst.clear();
            }
        } finally {
            recycler.release(dst);
        }

        super.finish(channel, superblock);

        superblock.fragment_entry_count(this.blockIdAllocator);
    }

    @Override
    protected void applyStartOffset(@NonNull Superblock superblock, long startOffset) {
        superblock.fragment_table_start(startOffset);
    }

    @Override
    public void close() throws IOException {
        this.indexChannel.close();
        Files.delete(this.indexFile);

        super.close();
    }
}
