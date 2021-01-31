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
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.ExtendedFileInode;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
final class FragmentTableBuilder extends MultilevelSquashfsBuilder {
    private final FragmentDatablockBuilder data;

    private final ByteBuf dataBuffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer();

    protected final Path indexFile;
    protected final FileChannel indexChannel;

    public FragmentTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        super(compression, root, parent);

        this.data = new FragmentDatablockBuilder(compression, root.resolve("data"), parent);

        this.indexChannel = FileChannel.open(this.indexFile = root.resolve("index"), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    public long appendFragmentData(@NonNull ByteBuf data) throws IOException {
        int readableBytes = this.dataBuffer.readableBytes();
        int blockSize = 1 << this.parent.blockLog;
        checkArg(readableBytes < blockSize);
        this.dataBuffer.writeBytes(data);

        if (this.dataBuffer.readableBytes() >= blockSize) {
            this.flushData(this.dataBuffer.readableBytes() == blockSize ? blockSize : readableBytes);
        }
    }

    private void flushData(int count) throws IOException {
        long blockIndex = this.data.putBlock(this.dataBuffer.readSlice(count));
        this.dataBuffer.discardSomeReadBytes();


    }

    public void writeFileContents(@NonNull ByteBuf data, @NonNull ExtendedFileInode.ExtendedFileInodeBuilder builder) throws IOException {
        IntList block_sizes = new IntArrayList();

        int blockSize = 1 << this.parent.blockLog;
        if (data.readableBytes() < blockSize) {
            builder.block_sizes(new int[0]);
        }

        builder.block_sizes(block_sizes.toIntArray());
    }

    public void append(@NonNull FragmentBlockEntry entry) throws IOException {
        this.writer.buffer.writeLongLE(entry.start)
                .writeIntLE(entry.size)
                .writeIntLE(0);
        this.writer.flush();
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        if (this.dataBuffer.isReadable()) {
            this.flushData(this.dataBuffer.readableBytes());
        }
        this.dataBuffer.release();

        this.data.finish(channel, superblock);

        super.finish(channel, superblock);

        superblock.fragment_entry_count(this.count);
    }

    @Override
    protected void applyStartOffset(@NonNull Superblock superblock, long startOffset) {
        superblock.fragment_table_start(startOffset);
    }

    @Override
    public void close() throws IOException {
        this.indexChannel.close();
        Files.delete(this.indexFile);

        this.data.close();

        super.close();
    }
}
