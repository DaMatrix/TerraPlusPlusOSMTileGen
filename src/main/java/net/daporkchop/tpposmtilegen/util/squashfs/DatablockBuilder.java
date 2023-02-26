/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
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
import lombok.NonNull;
import lombok.SneakyThrows;
import net.daporkchop.lib.common.function.io.IORunnable;
import net.daporkchop.lib.common.function.exception.ERunnable;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import org.omg.IOP.IOR;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
final class DatablockBuilder implements ISquashfsBuilder {
    protected final SquashfsBuilder parent;

    protected final Path root;

    protected final Path indexFile;
    protected final FileChannel indexChannel;

    protected final Semaphore lock = new Semaphore(PorkUtil.CPU_COUNT);

    protected long writtenBlocks = 0L;

    public DatablockBuilder(@NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;

        this.root = Files.createDirectories(root);

        this.indexFile = this.root.resolve("index");
        this.indexChannel = FileChannel.open(this.indexFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    public long writeBlocks(@NonNull ByteBuf buffer, int blocks, boolean sync) {
        int blockSize = 1 << this.parent.blockLog;
        checkArg(positive(buffer.readableBytes(), "readable bytes") > (positive(blocks, "blocks") - 1) * blockSize);

        long blockIndex = this.writtenBlocks;
        this.writtenBlocks += blocks;

        ByteBuf slice = buffer.readSlice(min(buffer.readableBytes(), blocks * blockSize)).copy();

        this.lock.acquireUninterruptibly();
        CompletableFuture<Void> future = CompletableFuture.runAsync((IORunnable) () -> {
            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf compressedBuffer = recycler.get();
            ByteBuf indexBuffer = recycler.get();
            try {
                for (int i = 0; i < blocks; i++) {
                    indexBuffer.writeIntLE(this.compress(slice.readSlice(min(slice.readableBytes(), blockSize)), compressedBuffer))
                            .writeLongLE(-1L);
                }

                synchronized (this.parent.channel) {
                    long pos = this.parent.channel.position();
                    for (int i = 0, off = 0; i < blocks; i++) {
                        indexBuffer.setLongLE(i * 12 + 4, pos + off);
                        off += indexBuffer.getIntLE(i * 12) & ~DATA_BLOCK_UNCOMPRESSED_FLAG;
                    }

                    writeFully(this.parent.channel, compressedBuffer);
                    writeFully(this.indexChannel, blockIndex * 12L, indexBuffer);
                }
            } finally {
                this.lock.release();
                recycler.release(indexBuffer);
                recycler.release(compressedBuffer);
                slice.release();
            }
        });
        if (sync) {
            future.join();
        }

        return blockIndex;
    }

    protected int compress(ByteBuf readBuffer, ByteBuf writeBuffer) throws IOException {
        int count = readBuffer.readableBytes();
        int oldWriteIndex = writeBuffer.writerIndex();
        this.parent.compression.compress(readBuffer, writeBuffer);

        if (writeBuffer.writerIndex() - oldWriteIndex >= count) {
            writeBuffer.writerIndex(oldWriteIndex).writeBytes(readBuffer.readerIndex(0));
            return count | DATA_BLOCK_UNCOMPRESSED_FLAG;
        }
        return writeBuffer.writerIndex() - oldWriteIndex;
    }

    @SneakyThrows(IOException.class)
    public DatablockReference getReference(long idx) {
        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf buf = recycler.get();
        try {
            readFully(this.indexChannel, idx * 12L, buf, 12);
            return new DatablockReference(buf.readIntLE(), buf.readLongLE());
        } finally {
            recycler.release(buf);
        }
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        this.lock.acquireUninterruptibly(PorkUtil.CPU_COUNT);
        this.lock.release(PorkUtil.CPU_COUNT);
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
    }

    @Override
    public void close() throws IOException {
        this.indexChannel.close();
        Files.delete(this.indexFile);

        Files.delete(this.root);
    }
}
