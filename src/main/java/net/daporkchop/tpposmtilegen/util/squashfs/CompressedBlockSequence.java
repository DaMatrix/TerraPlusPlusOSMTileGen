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
import lombok.NonNull;
import lombok.SneakyThrows;
import net.daporkchop.lib.common.function.io.IORunnable;
import net.daporkchop.tpposmtilegen.util.CloseableExecutor;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;

/**
 * @author DaPorkchop_
 */
abstract class CompressedBlockSequence implements ISquashfsBuilder, Iterable<DatablockReference> {
    protected final SquashfsBuilder parent;
    protected final Compression compression;

    protected final Path root;

    protected final Path rawFile;
    protected final FileChannel rawChannel;
    protected final Path indexFile;
    protected final FileChannel indexChannel;

    protected final IntList blockSizes = new IntArrayList();

    protected final int blockSize;

    public CompressedBlockSequence(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;
        this.compression = compression;

        this.blockSize = this.blockSize0();

        this.root = Files.createDirectories(root);

        this.rawChannel = FileChannel.open(this.rawFile = root.resolve("raw"), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        this.indexChannel = FileChannel.open(this.indexFile = root.resolve("index"), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    protected abstract String name();

    protected abstract int blockSize0();

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
    }

    @Override
    @SneakyThrows(Exception.class)
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        CompletableFuture<Void> previousFuture = CompletableFuture.completedFuture(null);

        try (CloseableExecutor executor = new CloseableExecutor()) {
            long size = this.rawChannel.position();
            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Compress " + this.name())
                    .slot("blocks", (size - 1L) / this.blockSize + 1L).build()) {
                long _pos = 0L;
                for (int blockSize : this.blockSizes) {
                    long pos = _pos;
                    CompletableFuture<Void> thePreviousFuture = previousFuture;

                    previousFuture = CompletableFuture.runAsync((IORunnable) () -> {
                        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
                        ByteBuf readBuffer = recycler.get();
                        ByteBuf writeBuffer = recycler.get();
                        ByteBuf indexBuffer = recycler.get();
                        try {
                            readFully(this.rawChannel, pos, readBuffer, blockSize);
                            boolean uncompressed = this.compress(readBuffer, writeBuffer);
                            indexBuffer.writeIntLE(this.toReferenceSize(uncompressed, writeBuffer.readableBytes()));

                            thePreviousFuture.join();

                            indexBuffer.writeLongLE(channel.position());
                            writeFully(channel, writeBuffer);
                            writeFully(this.indexChannel, indexBuffer);
                        } finally {
                            recycler.release(indexBuffer);
                            recycler.release(writeBuffer);
                            recycler.release(readBuffer);
                        }
                        notifier.step(0);
                    }, executor);

                    _pos += blockSize;
                }
                previousFuture.join();
            }
        }
    }

    protected boolean compress(ByteBuf readBuffer, ByteBuf writeBuffer) throws IOException {
        int origSize = readBuffer.readableBytes();
        this.compression.compress(readBuffer, writeBuffer);

        if (writeBuffer.readableBytes() >= origSize) {
            writeBuffer.writerIndex(0).writeBytes(readBuffer.readerIndex(0));
            return true;
        }
        return false;
    }

    protected abstract int toReferenceSize(boolean uncompressed, int compressedSize);

    public int writtenBlockCount() {
        return this.blockSizes.size();
    }

    public int putBlock(@NonNull ByteBuf buf) throws IOException {
        checkArg(buf.readableBytes() <= this.blockSize, buf.readableBytes());
        int idx = this.writtenBlockCount();
        this.blockSizes.add(buf.readableBytes());
        writeFully(this.rawChannel, buf);
        return idx;
    }

    @SneakyThrows(IOException.class)
    public DatablockReference getReference(int idx) {
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
    public Iterator<DatablockReference> iterator() {
        return IntStream.range(0, this.writtenBlockCount()).mapToObj(this::getReference).iterator();
    }

    @Override
    public void close() throws IOException {
        this.rawChannel.close();
        Files.delete(this.rawFile);

        this.indexChannel.close();
        Files.delete(this.indexFile);

        Files.delete(this.root);
    }
}
