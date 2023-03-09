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
import lombok.NonNull;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.function.exception.ESupplier;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.math.PMath;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import net.daporkchop.tpposmtilegen.util.Utils;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public final class UInt64BlobUnsortedWriteAccess extends AbstractUnsortedWriteAccess {
    private final Handle<Path> indexPathHandle;
    private final FileChannel indexChannel;

    private final LongAdder totalDataSize = new LongAdder();
    private final CloseableThreadLocal<WriteBuffer> writeBuffers = CloseableThreadLocal.of(WriteBuffer::new);

    private final double averageCompressedSizePerKey;

    public UInt64BlobUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, double averageCompressedSizePerKey) throws Exception {
        super(storage, columnFamilyHandle);
        this.averageCompressedSizePerKey = averageCompressedSizePerKey;

        this.indexPathHandle = storage.getTmpFilePath(
                UInt64BlobUnsortedWriteAccess.class.getSimpleName() + '-' + new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), "buf");
        this.indexChannel = FileChannel.open(this.indexPathHandle.get(), READ, WRITE, CREATE_NEW);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        this.totalDataSize.add(4L + value.remaining());

        long valueAddr = Memory.malloc(4L + value.remaining());
        PUnsafe.putInt(valueAddr, value.remaining());
        Memory.memcpy(valueAddr + 4L, PUnsafe.pork_directBufferAddress(value) + value.position(), value.remaining());

        this.writeBuffers.get().put(realKey, valueAddr);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.totalDataSize.sum();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.getDataSize() != 0L || this.writeBuffers.snapshotValues().stream().anyMatch(writeBuffer -> writeBuffer.buf.isReadable());
    }

    @Override
    protected void flush0() throws Exception {
        this.writeBuffers.forEach((EConsumer<WriteBuffer>) WriteBuffer::flush);

        long size = this.indexChannel.size();
        if (size > 0L) {
            try (MemoryMap mmap = new MemoryMap(this.indexChannel, FileChannel.MapMode.READ_WRITE, 0L, size)) {
                //sort the buffer's contents
                try (TimedOperation sortOperation = new TimedOperation("Sort buffer", this.logger)) {
                    sortBuffer(mmap.addr(), size, true);
                }

                //split the buffer's contents and build SST files out of it
                List<Handle<Path>> paths;
                try (TimedOperation sstOperation = new TimedOperation("Build SST files", this.logger);
                     Options options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions())) {
                    EnvOptions envOptions = this.storage.db().config().envOptions();

                    final long targetBlockSize = PMath.roundUp((long) (16L
                                                                       * this.averageCompressedSizePerKey
                                                                       * options.targetFileSizeBase()), Math.max(16, PUnsafe.pageSize()));
                    long[] blockStartAddrs = LongStream.rangeClosed(0L, size / targetBlockSize)
                            .flatMap(i -> LongStream.of(mmap.addr() + i * targetBlockSize, Math.min(targetBlockSize, size - i * targetBlockSize)))
                            .toArray();

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

                int totalCount = paths.size();
                long totalSize = paths.stream().map(Handle::get).map((IOFunction<Path, Long>) Files::size).mapToLong(Long::longValue).sum();

                //ingest the SST files
                try (TimedOperation ingestOperation = new TimedOperation("Ingest SST files", this.logger)) {
                    this.storage.db().delegate().ingestExternalFile(this.columnFamilyHandle,
                            paths.stream().map(Handle::get).map(Path::toString).collect(Collectors.toList()),
                            this.storage.db().config().ingestOptions(DatabaseConfig.IngestType.MOVE));
                    paths.forEach(Handle::release);
                    paths.clear();
                }

                this.logger.success("ingested %d keys totalling %d bytes (%.2f MiB) in %d SST files totalling %d bytes (%.2f MiB)",
                        size / 16L,
                        this.totalDataSize.sum(), this.totalDataSize.sumThenReset() / (1024.0d * 1024.0d),
                        totalCount, totalSize, totalSize / (1024.0d * 1024.0d));
            }

            //now that all the data has been appended and ingested, truncate the file down to 0 bytes to reset everything
            this.indexChannel.truncate(0L);
        }
    }

    private static native void sortBuffer(long addr, long size, boolean parallel) throws OutOfMemoryError;

    private static native long appendKeys(long writerHandle, long begin, long end);

    private Handle<Path> buildSstFileFromRange(@NonNull EnvOptions envOptions, @NonNull Options options, long indexAddr, long indexSize) throws Exception {
        checkArg(indexSize % 16L == 0L, indexSize);
        checkArg(indexSize != 0L, indexSize);

        Handle<Path> pathHandle = this.storage.getTmpFilePath(new String(this.columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sst");

        try (SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(pathHandle.get().toString());

            long writtenKeys = appendKeys(writer.getNativeHandle(), indexAddr, indexAddr + indexSize);
            checkState(writtenKeys != 0L, writtenKeys);

            writer.finish();
        }

        return pathHandle;
    }

    @Override
    public synchronized void close() throws Exception {
        this.writeBuffers.close();

        super.close();

        this.indexChannel.close();
        PFiles.rm(this.indexPathHandle.get());
        this.indexPathHandle.release();
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
                synchronized (UInt64BlobUnsortedWriteAccess.this) {
                    Utils.writeFully(UInt64BlobUnsortedWriteAccess.this.indexChannel, this.buf);
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
