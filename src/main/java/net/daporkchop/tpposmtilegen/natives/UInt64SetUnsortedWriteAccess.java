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

package net.daporkchop.tpposmtilegen.natives;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.function.exception.ESupplier;
import net.daporkchop.lib.common.math.PMath;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public final class UInt64SetUnsortedWriteAccess implements DBWriteAccess {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
        init();
    }

    private static native void init();

    private final Storage storage;
    private final ColumnFamilyHandle columnFamilyHandle;

    private final Handle<Path> pathHandle;
    private final FileChannel channel;

    private final Handle<Path> sortedPathHandle;
    private final FileChannel sortedChannel;

    private final CloseableThreadLocal<WriteBuffer> writeBuffers = CloseableThreadLocal.of(WriteBuffer::new);

    private final double compressionRatio;
    private final boolean merge;

    private volatile boolean flushing = false;

    public UInt64SetUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, boolean assumeEmpty, double compressionRatio) throws Exception {
        this.storage = storage;
        this.columnFamilyHandle = columnFamilyHandle;
        this.compressionRatio = compressionRatio;
        this.merge = !assumeEmpty;

        this.pathHandle = storage.getTmpFilePath(
                UInt64SetUnsortedWriteAccess.class.getSimpleName() + '-' + new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), "buf");
        this.channel = FileChannel.open(this.pathHandle.get(), READ, WRITE, CREATE_NEW);

        this.sortedPathHandle = storage.getTmpFilePath(
                UInt64SetUnsortedWriteAccess.class.getSimpleName() + '-' + new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sorted.buf");
        this.sortedChannel = FileChannel.open(this.sortedPathHandle.get(), READ, WRITE, CREATE_NEW, SPARSE);

        /*Runtime.getRuntime()
                .exec(new String[]{ "chattr", "+C", this.pathHandle.get().toAbsolutePath().toString(), this.sortedPathHandle.get().toAbsolutePath().toString() })
                .waitFor();*/
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
        checkArg(UInt64SetMergeOperator.getNumDels(value) == 0L, "delete isn't supported!");

        checkArg(key.length == 8, key.length);
        long realKey = PUnsafe.getUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0));

        long numAdds = UInt64SetMergeOperator.getNumAdds(value);
        if (numAdds == 0L) {
            return;
        }

        WriteBuffer writeBuffer = this.writeBuffers.get();
        for (long i = 0L; i < numAdds; i++) {
            writeBuffer.put(realKey, UInt64SetMergeOperator.getIndexedAdd(value, i));
        }
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDataSize() throws Exception {
        return this.channel.size();
    }

    @Override
    public boolean isDirty() throws Exception {
        return DBWriteAccess.super.isDirty() || this.writeBuffers.snapshotValues().stream().anyMatch(writeBuffer -> writeBuffer.buf.isReadable());
    }

    @Override
    public synchronized void flush() throws Exception {
        this.flushing = true;

        this.writeBuffers.forEach((EConsumer<WriteBuffer>) WriteBuffer::flush);

        long size = this.channel.size();
        if (size > 0L) {
            this.sortedChannel.write(ByteBuffer.allocate(1), size - 1L);
            try (MemoryMap srcMmap = new MemoryMap(this.channel, FileChannel.MapMode.READ_WRITE, 0L, size);
                 MemoryMap sortedMmap = new MemoryMap(this.sortedChannel, FileChannel.MapMode.READ_WRITE, 0L, size)) {
                //sort the buffer's contents
                try (TimedOperation sortOperation = new TimedOperation("Sort buffer")) {
                    mergeSortBufferNWay(srcMmap.addr(), size, null, sortedMmap.addr(), size);
                }

                //split the buffer's contents and build SST files out of it
                List<Handle<Path>> paths;
                try (TimedOperation sstOperation = new TimedOperation("Build SST files");
                     Options options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions())) {
                    EnvOptions envOptions = this.storage.db().config().envOptions();

                    final long targetBlockSize = PMath.roundUp((long) (this.compressionRatio * options.targetFileSizeBase()), 16L);
                    long[] blockStartAddrs = partitionSortedRange(sortedMmap.addr(), size, targetBlockSize);

                    Memory.madvise(sortedMmap.addr(), size, Memory.Usage.MADV_SEQUENTIAL);

                    CompletableFuture<Handle<Path>>[] pathFutures = uncheckedCast(new CompletableFuture[blockStartAddrs.length >> 1]);
                    for (int i = 0; i < blockStartAddrs.length; i += 2) {
                        long blockAddr = blockStartAddrs[i];
                        long blockSize = blockStartAddrs[i + 1];

                        pathFutures[i >> 1] = CompletableFuture.supplyAsync(
                                (ESupplier<Handle<Path>>) () -> this.buildSstFileFromRange(envOptions, options, blockAddr, blockSize));
                    }

                    CompletableFuture.allOf(pathFutures);
                    try {
                        paths = Stream.of(pathFutures).map(CompletableFuture::join).collect(Collectors.toList());
                    } catch (Exception e) {
                        while (!Stream.of(pathFutures).allMatch(CompletableFuture::isDone)) {
                            try {
                                Stream.of(pathFutures).filter(f -> !f.isDone()).forEach(CompletableFuture::join);
                            } catch (Exception e1) {
                                e1.addSuppressed(e);
                            }
                        }
                        throw e;
                    }
                }

                //ingest the SST files
                try (TimedOperation ingestOperation = new TimedOperation("Ingest SST files")) {
                    this.storage.db().delegate().ingestExternalFile(this.columnFamilyHandle,
                            paths.stream().map(Handle::get).map(Path::toString).collect(Collectors.toList()),
                            this.storage.db().config().ingestOptions(DatabaseConfig.IngestType.MOVE));
                    paths.forEach(Handle::release);
                    paths.clear();
                }
            }

            //now that all the data has been appended and ingested, truncate the file down to 0 bytes to reset everything
            this.channel.truncate(0L);
            this.sortedChannel.truncate(0L);
        }

        this.flushing = false;
    }

    private static native void sortBuffer(long addr, long size, boolean parallel) throws OutOfMemoryError;

    private static native boolean isSorted(long addr, long size, boolean parallel) throws OutOfMemoryError;

    private static native boolean nWayMerge(long[] srcAddrs, long[] srcSizes, int srcCount, long dstAddr, long dstSize) throws OutOfMemoryError;

    private static native long[] partitionSortedRange(long addr, long size, long targetBlockSize) throws OutOfMemoryError;

    private static void mergeSortBufferNWay(long addr, long size, Runnable flush, long dstAddr, long dstSize) {
        checkArg(size == dstSize);
        flush = fallbackIfNull(flush, (Runnable) () -> {});

        final long maxInitialSortSize = 8L << 30L;

        //break the buffer up into smaller runs, then merge them
        LongList addrs = new LongArrayList();
        LongList sizes = new LongArrayList();
        for (long offset = 0L; offset < size; offset += maxInitialSortSize) {
            long runSize = Math.min(maxInitialSortSize, size - offset);
            if (!isSorted(addr + offset, runSize, true)) {
                sortBuffer(addr + offset, runSize, true);
                checkState(isSorted(addr + offset, runSize, true));
                flush.run();
                logger.info("sorted %dth initial block at offset 0x%x", offset / maxInitialSortSize, offset);
            } else {
                logger.info("skipping sorted %dth initial block at offset 0x%x", offset / maxInitialSortSize, offset);
            }

            addrs.add(addr + offset);
            sizes.add(runSize);
        }

        Memory.madvise(dstAddr, dstSize, Memory.Usage.MADV_REMOVE);

        try (TimedOperation merge = new TimedOperation("Merge " + addrs.size() + " sorted arrays")) {
            nWayMerge(addrs.toLongArray(), sizes.toLongArray(), addrs.size(), dstAddr, dstSize);
        }
        try (TimedOperation verify = new TimedOperation("Verify sorted data")) {
            checkState(isSorted(dstAddr, dstSize, true));
        }

        Memory.madvise(addr, size, Memory.Usage.MADV_REMOVE);
    }

    private static native long combineAndAppendKey(long writerHandle, long begin, long end, boolean merge);

    private Handle<Path> buildSstFileFromRange(@NonNull EnvOptions envOptions, @NonNull Options options, long addr, long size) throws Exception {
        checkArg(size % 16L == 0L, size);

        Handle<Path> pathHandle = this.storage.getTmpFilePath(new String(this.columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sst");

        try (SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(pathHandle.get().toString());

            for (long firstAddr = addr, endAddr = addr + size; firstAddr < endAddr; ) {
                firstAddr = combineAndAppendKey(writer.getNativeHandle(), firstAddr, endAddr, this.merge);
            }

            writer.finish();
        }

        return pathHandle;
    }

    @Override
    public synchronized void clear() throws Exception {
        this.channel.truncate(0L);
    }

    @Override
    public synchronized void close() throws Exception {
        this.writeBuffers.close();
        this.flush();

        this.channel.close();
        PFiles.rm(this.pathHandle.get());
        this.pathHandle.release();

        this.sortedChannel.close();
        PFiles.rm(this.sortedPathHandle.get());
        this.sortedPathHandle.release();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }

    protected class WriteBuffer implements Flushable, AutoCloseable {
        protected final ByteBuf buf = Unpooled.directBuffer(1 << 20, 1 << 20);

        public synchronized void put(long key, long value) throws IOException {
            if (!this.buf.isWritable(16)) {
                this.flush();
            }

            this.buf.writeLongLE(key).writeLongLE(value);
        }

        @Override
        public synchronized void flush() throws IOException {
            if (this.buf.isReadable()) {
                synchronized (UInt64SetUnsortedWriteAccess.this) {
                    do {
                        boolean interrupted = Thread.interrupted();
                        this.buf.readBytes(UInt64SetUnsortedWriteAccess.this.channel, this.buf.readableBytes());
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    } while (this.buf.isReadable());
                }
                this.buf.clear();
            }
        }

        @Override
        public synchronized void close() throws Exception {
            this.flush();
            this.buf.release();
        }
    }
}
