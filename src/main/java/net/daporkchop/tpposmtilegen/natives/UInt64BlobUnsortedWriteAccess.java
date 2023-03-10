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
import java.util.concurrent.atomic.AtomicLong;
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

    private final Handle<Path> dataPathHandle;
    private final FileChannel dataChannel;

    private final AtomicLong dataOffsetAlloc = new AtomicLong();

    private final LongAdder totalDataSize = new LongAdder();
    private final CloseableThreadLocal<WriteBuffer> writeBuffers = CloseableThreadLocal.of(WriteBuffer::new);

    private final double averageCompressedSizePerKey;

    public UInt64BlobUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, double averageCompressedSizePerKey) throws Exception {
        super(storage, columnFamilyHandle);
        this.averageCompressedSizePerKey = averageCompressedSizePerKey;

        this.indexPathHandle = storage.getTmpFilePath(
                UInt64BlobUnsortedWriteAccess.class.getSimpleName() + '-' + this.columnFamilyHandle(), "index.buf");
        this.indexChannel = FileChannel.open(this.indexPathHandle.get(), READ, WRITE, CREATE_NEW);

        this.dataPathHandle = storage.getTmpFilePath(
                UInt64BlobUnsortedWriteAccess.class.getSimpleName() + '-' + this.columnFamilyHandle(), "data.buf");
        this.dataChannel = FileChannel.open(this.dataPathHandle.get(), READ, WRITE, CREATE_NEW);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        this.totalDataSize.add(4L + value.remaining());

        this.writeBuffers.get().put(realKey, Unpooled.wrappedBuffer(value));
    }

    @Override
    public long getDataSize() throws Exception {
        return this.totalDataSize.sum();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.getDataSize() != 0L || this.writeBuffers.snapshotValues().stream().anyMatch(writeBuffer -> writeBuffer.indexBuf.isReadable());
    }

    @Override
    protected void flush0() throws Exception {
        this.writeBuffers.forEach((EConsumer<WriteBuffer>) WriteBuffer::flush);

        long size = this.indexChannel.size();
        if (size > 0L) {
            try (MemoryMap indexMmap = new MemoryMap(this.indexChannel, FileChannel.MapMode.READ_WRITE, 0L, size);
                 MemoryMap dataMmap = new MemoryMap(this.dataChannel, FileChannel.MapMode.READ_ONLY, 0L, this.dataChannel.size())) {
                //sort the buffer's contents
                try (TimedOperation sortOperation = new TimedOperation("Sort buffer", this.logger)) {
                    sortBuffer(indexMmap.addr(), size, true);
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
                            .flatMap(i -> LongStream.of(indexMmap.addr() + i * targetBlockSize, Math.min(targetBlockSize, size - i * targetBlockSize)))
                            .toArray();

                    CompletableFuture<Handle<Path>>[] pathFutures = uncheckedCast(new CompletableFuture[blockStartAddrs.length >> 1]);
                    for (int i = 0; i < blockStartAddrs.length; i += 2) {
                        long blockAddr = blockStartAddrs[i];
                        long blockSize = blockStartAddrs[i + 1];

                        pathFutures[i >> 1] = CompletableFuture.supplyAsync(
                                (ESupplier<Handle<Path>>) () -> this.buildSstFileFromRange(envOptions, options, blockAddr, blockSize, dataMmap.addr()));
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

            //now that all the data has been appended and ingested, truncate the files down to 0 bytes to reset everything
            Stream.of(this.indexChannel, this.dataChannel).parallel().forEach((EConsumer<FileChannel>) c -> c.truncate(0L));
        }
    }

    private static native void sortBuffer(long addr, long size, boolean parallel) throws OutOfMemoryError;

    private static native long appendKeys(long writerHandle, long begin, long end, long dataBaseAddr);

    private Handle<Path> buildSstFileFromRange(@NonNull EnvOptions envOptions, @NonNull Options options, long indexAddr, long indexSize, long dataBaseAddr) throws Exception {
        checkArg(indexSize % 16L == 0L, indexSize);
        checkArg(indexSize != 0L, indexSize);

        Handle<Path> pathHandle = this.storage.getTmpFilePath(new String(this.columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sst");

        try (SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(pathHandle.get().toString());

            long writtenKeys = appendKeys(writer.getNativeHandle(), indexAddr, indexAddr + indexSize, dataBaseAddr);
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

        this.dataChannel.close();
        PFiles.rm(this.dataPathHandle.get());
        this.dataPathHandle.release();
    }

    protected class WriteBuffer implements Flushable, AutoCloseable {
        private static final int INDEX_BLOCK_SIZE = 1 << 20;
        private static final int DATA_BLOCK_SIZE = 1 << 20;

        protected final ByteBuf indexBuf = Unpooled.directBuffer(INDEX_BLOCK_SIZE, INDEX_BLOCK_SIZE);

        protected long dataTargetOffset = -1L;
        protected final ByteBuf dataBuf = Unpooled.directBuffer(DATA_BLOCK_SIZE, DATA_BLOCK_SIZE);

        public synchronized void put(long key, @NonNull ByteBuf value) throws IOException {
            long dataOffset = this.putData(value);

            if (!this.indexBuf.isWritable(16)) {
                this.flushIndex();
            }

            this.indexBuf.writeLongLE(key).writeLongLE(dataOffset);
        }

        private synchronized long putData(@NonNull ByteBuf data) throws IOException {
            int totalSpace = Math.addExact(data.readableBytes(), 4);

            if (totalSpace >= DATA_BLOCK_SIZE) { //we don't have enough space to buffer the data, write it directly out to disk
                long targetOffset = UInt64BlobUnsortedWriteAccess.this.dataOffsetAlloc.getAndAdd(totalSpace);
                ByteBuf lengthPrefix = Unpooled.buffer(4).writeIntLE(data.readableBytes());
                checkState(lengthPrefix.readableBytes() == 4);
                Utils.writeFully(UInt64BlobUnsortedWriteAccess.this.dataChannel, targetOffset, lengthPrefix);
                Utils.writeFully(UInt64BlobUnsortedWriteAccess.this.dataChannel, targetOffset + 4L, data);
                lengthPrefix.release();
                return targetOffset;
            } else if (!this.dataBuf.isWritable(totalSpace)) {
                this.flushData();
            }

            if (this.dataTargetOffset < 0L) { //assign a new target offset
                checkState(!this.dataBuf.isReadable());
                this.dataBuf.clear();

                this.dataTargetOffset = UInt64BlobUnsortedWriteAccess.this.dataOffsetAlloc.getAndAdd(DATA_BLOCK_SIZE);
            }

            long targetOffset = this.dataTargetOffset + this.dataBuf.writerIndex();
            this.dataBuf.writeIntLE(data.readableBytes()).writeBytes(data);
            return targetOffset;
        }

        @Override
        public synchronized void flush() throws IOException {
            this.flushIndex();
            this.flushData();
        }

        private synchronized void flushIndex() throws IOException {
            if (this.indexBuf.isReadable()) {
                synchronized (UInt64BlobUnsortedWriteAccess.this) {
                    Utils.writeFully(UInt64BlobUnsortedWriteAccess.this.indexChannel, this.indexBuf);
                }
                this.indexBuf.clear();
            }
        }

        private synchronized void flushData() throws IOException {
            if (this.dataBuf.isReadable()) {
                checkState(this.dataTargetOffset >= 0L);
                Utils.writeFully(UInt64BlobUnsortedWriteAccess.this.dataChannel, this.dataTargetOffset, this.dataBuf);
                this.dataTargetOffset = -1L;
                this.dataBuf.clear();
            }
        }

        @Override
        public synchronized void close() throws Exception {
            this.flush();
            this.indexBuf.release();
            this.dataBuf.release();
        }
    }
}
