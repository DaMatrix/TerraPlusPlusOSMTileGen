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
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.exception.ERunnable;
import net.daporkchop.lib.common.function.exception.ESupplier;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.primitive.map.LongLongMap;
import net.daporkchop.lib.primitive.map.LongObjMap;
import net.daporkchop.lib.primitive.map.concurrent.LongLongConcurrentHashMap;
import net.daporkchop.lib.primitive.map.concurrent.LongObjConcurrentHashMap;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
public final class UInt64ToBlobMapUnsortedWriteAccess extends AbstractUnsortedWriteAccess {
    private final Handle<Path> dataPathHandle;
    private final FileChannel dataChannel;
    private final MemoryMap dataMmap;
    private final long dataSize = 1L << 40L;

    private final AtomicLong dataAddrAllocator = new AtomicLong();
    private final LongAdder totalDataWritten = new LongAdder();

    private final LongObjMap<TileBuffer> keysToBuffers = new LongObjConcurrentHashMap<>();

    private final double compressionRatio;

    public UInt64ToBlobMapUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, double compressionRatio) throws Exception {
        super(storage, columnFamilyHandle);
        this.compressionRatio = compressionRatio;

        this.dataPathHandle = storage.getTmpFilePath(
                UInt64ToBlobMapUnsortedWriteAccess.class.getSimpleName() + '-' + this.columnFamilyName, "buf");
        this.dataChannel = FileChannel.open(this.dataPathHandle.get(), READ, WRITE, CREATE_NEW);
        Utils.truncate(this.dataChannel, this.dataSize);
        this.dataMmap = new MemoryMap(this.dataChannel, FileChannel.MapMode.READ_WRITE, 0L, this.dataSize);
        this.dataAddrAllocator.set(this.dataMmap.addr());
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        UInt64ToBlobMapMergeOperator.decodeToSlices(Unpooled.wrappedBuffer(value), (entryKey, data) -> {
            this.totalDataWritten.add(8L + 8L + 4L + data.readableBytes());

            try {
                this.keysToBuffers.computeIfAbsent(realKey, TileBuffer::new).put(entryKey, data);
            } catch (IOException e) {
                throw PUnsafe.throwException(e);
            }
        });
    }

    @Override
    public long getDataSize() throws Exception {
        return this.totalDataWritten.sum();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.getDataSize() != 0L;
    }

    @Override
    protected void flush0() throws Exception {
        long size = this.getDataSize();
        if (size == 0L) {
            return;
        }

        LongAdder reportedTotalSize = new LongAdder();

        //split the buffer's contents and build SST files out of it
        List<Handle<Path>> paths;
        try (TimedOperation sstOperation = new TimedOperation("Build SST files", this.logger);
             Options options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions())) {
            EnvOptions envOptions = this.storage.db().config().envOptions();

            final long targetBlockSize = (long) (this.compressionRatio * options.targetFileSizeBase());
            List<List<TileBuffer>> batches = new ArrayList<>();

            {
                List<TileBuffer> currentBatch = null;
                long currentBatchSize = 0L;
                for (TileBuffer entry : this.keysToBuffers.values().stream()
                        .sorted((a, b) -> Long.compareUnsigned(a.key, b.key))
                        .collect(Collectors.toList())) {
                    if (currentBatch == null) {
                        currentBatch = new ArrayList<>();
                        batches.add(currentBatch);
                    }

                    currentBatch.add(entry);
                    currentBatchSize += entry.totalDataSize;

                    if (currentBatchSize >= targetBlockSize) {
                        currentBatch = null;
                        currentBatchSize = 0L;
                    }
                }
            }

            CompletableFuture<Handle<Path>>[] pathFutures = uncheckedCast(batches.stream()
                    .map(batch -> CompletableFuture.supplyAsync(
                            (ESupplier<Handle<Path>>) () -> this.buildSstFileFromRange(envOptions, options, batch, reportedTotalSize)))
                    .toArray(CompletableFuture[]::new));

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

        CompletableFuture<?> truncateFuture = CompletableFuture.runAsync((ERunnable) () -> this.dataChannel.truncate(0L));

        //ingest the SST files
        try (TimedOperation ingestOperation = new TimedOperation("Ingest SST files", this.logger)) {
            this.storage.db().delegate().ingestExternalFile(this.columnFamilyHandle,
                    paths.stream().map(Handle::get).map(Path::toString).collect(Collectors.toList()),
                    this.storage.db().config().ingestOptions(DatabaseConfig.IngestType.MOVE));
            paths.forEach(Handle::release);
            paths.clear();
        }

        this.logger.success("ingested %d keys totalling %d bytes (%.2f MiB) (reported: %d bytes (%.2f MiB)) in %d SST files totalling %d bytes (%.2f MiB)",
                size / 16L,
                this.getDataSize(), this.getDataSize() / (1024.0d * 1024.0d),
                reportedTotalSize.sum(), reportedTotalSize.sum() / (1024.0d * 1024.0d),
                totalCount, totalSize, totalSize / (1024.0d * 1024.0d));

        truncateFuture.join();
    }

    private static native long appendKey(long writerHandle, long key, long root);

    private Handle<Path> buildSstFileFromRange(@NonNull EnvOptions envOptions, @NonNull Options options, @NonNull List<TileBuffer> batch, @NonNull LongAdder reportedTotalSize) throws Exception {
        checkArg(!batch.isEmpty());

        Handle<Path> pathHandle = this.storage.getTmpFilePath(this.columnFamilyName, "sst");

        try (SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(pathHandle.get().toString());

            for (TileBuffer buffer : batch) {
                //checkState(buffer.firstDataOffset >= 0L);
                //long writtenData = appendKey(writer.getNativeHandle(), buffer.key, dataBaseAddr + buffer.firstDataOffset, dataBaseAddr);
                checkState(buffer.firstDataAddr != 0L);
                long writtenData = appendKey(writer.getNativeHandle(), buffer.key, buffer.firstDataAddr);
                checkState(writtenData != 0L, writtenData);
                reportedTotalSize.add(writtenData);
            }

            writer.finish();
        }

        return pathHandle;
    }

    @Override
    public synchronized void close() throws Exception {
        super.close();

        this.dataMmap.close();
        this.dataChannel.close();
        PFiles.rm(this.dataPathHandle.get());
        this.dataPathHandle.release();
    }

    @RequiredArgsConstructor
    private class TileBuffer {
        private static final int DATA_BLOCK_SIZE = 1 << 20;

        private final long key;

        private long totalDataSize = 0L;
        private long firstDataAddr = 0L;

        private long currentWriterIndex = 0L;
        private long currentBlockEnd = 0L;
        private long prevDataNextAddr = 0L;

        /*
         * struct entry_t {
         *   entry_t* next;
         *   uint64_t key;
         *   int32_t value_size;
         *   char value_data[];
         * };
         */

        public synchronized void put(long key, @NonNull ByteBuf data) throws IOException {
            int totalSize = Math.addExact(8 + 8 + 4, data.readableBytes());
            this.totalDataSize += totalSize;

            long newEntryAddr;

            if (this.currentWriterIndex == 0L || this.currentBlockEnd - this.currentWriterIndex < totalSize) { //we can't write out to the current buffer, begin a new one
                if (totalSize >= DATA_BLOCK_SIZE) { //the data is too large to fit in an ordinary block, write it out as an individual larger one
                    newEntryAddr = UInt64ToBlobMapUnsortedWriteAccess.this.dataAddrAllocator.getAndAdd(totalSize);
                } else { //start a new block
                    this.currentWriterIndex = UInt64ToBlobMapUnsortedWriteAccess.this.dataAddrAllocator.getAndAdd(DATA_BLOCK_SIZE);
                    this.currentBlockEnd = this.currentWriterIndex + DATA_BLOCK_SIZE;

                    newEntryAddr = this.currentWriterIndex;
                    this.currentWriterIndex += totalSize;
                }

                if (this.firstDataAddr == 0L) {
                    this.firstDataAddr = newEntryAddr;
                }
            } else { //re-use the current buffer
                newEntryAddr = this.currentWriterIndex;
                this.currentWriterIndex += totalSize;
            }

            if (this.prevDataNextAddr != 0L) { //update the "next" pointer in the previous entry
                checkState(PUnsafe.getUnalignedLong(this.prevDataNextAddr) == 0L);
                PUnsafe.putUnalignedLong(this.prevDataNextAddr, newEntryAddr);
            }
            this.prevDataNextAddr = newEntryAddr;

            PUnsafe.putUnalignedLong(newEntryAddr, 0L); //next
            PUnsafe.putUnalignedLongLE(newEntryAddr + 8L, key); //key
            PUnsafe.putUnalignedIntLE(newEntryAddr + 8L + 8L, data.readableBytes()); //data_size
            Memory.memcpy(newEntryAddr + 8L + 8L + 4L, data.memoryAddress() + data.readerIndex(), data.readableBytes());
        }
    }

    /*@RequiredArgsConstructor
    private class TileBuffer implements Flushable, AutoCloseable {
        private static final int DATA_BLOCK_SIZE = 1 << 20;

        private final long key;

        private long totalDataSize = 0L;
        private long firstDataOffset = -1L;

        private long dataTargetOffset = -1L;
        private ByteBuf dataBuf;
        private int prevDataNextPointerOffset = -1;

        /*
         * struct entry_t {
         *   entry_t* next;
         *   uint64_t key;
         *   int32_t value_size;
         *   char value_data[];
         * };
         * /

        public synchronized void put(long key, @NonNull ByteBuf data) throws IOException {
            checkState(this.dataBuf == null || this.dataTargetOffset >= 0L);
            checkState(this.dataBuf == null || this.firstDataOffset >= 0L);

            int totalSize = Math.addExact(8 + 8 + 4, data.readableBytes());
            this.totalDataSize += totalSize;

            long nextDataTargetOffset;
            ByteBuf nextDataBuf;

            if (this.dataBuf == null || this.dataBuf.ensureWritable(totalSize, false) == 1) { //we can't write out to the current buffer, begin a new one
                if (totalSize >= DATA_BLOCK_SIZE) { //the data is too large to fit in an ordinary block, write it out as part of a larger one
                    nextDataBuf = Unpooled.directBuffer(totalSize, totalSize);
                } else {
                    nextDataBuf = Unpooled.directBuffer(totalSize, DATA_BLOCK_SIZE);
                }

                nextDataTargetOffset = UInt64ToBlobMapUnsortedWriteAccess.this.dataOffsetAllocator.getAndAdd(nextDataBuf.maxCapacity());
                if (this.firstDataOffset < 0L) {
                    checkState(this.dataBuf == null);
                    this.firstDataOffset = nextDataTargetOffset;
                }
            } else { //re-use the current buffer
                nextDataTargetOffset = this.dataTargetOffset;
                nextDataBuf = this.dataBuf;
            }

            checkState(nextDataTargetOffset >= 0L);
            checkState(nextDataBuf != null);

            int writtenEntryIndex = nextDataBuf.writerIndex();
            nextDataBuf.writeLongLE(0L)
                    .writeLongLE(key)
                    .writeIntLE(data.readableBytes())
                    .writeBytes(data);

            this.finishPut(nextDataTargetOffset, nextDataBuf, writtenEntryIndex);
        }

        private synchronized void finishPut(long nextDataTargetOffset, @NonNull ByteBuf nextDataBuf, int writtenEntryIndex) throws IOException {
            if (this.prevDataNextPointerOffset >= 0) {
                this.dataBuf.setLongLE(this.prevDataNextPointerOffset, nextDataTargetOffset + writtenEntryIndex);
                this.prevDataNextPointerOffset = -1;
            }

            if (this.dataTargetOffset == nextDataTargetOffset) { //re-using the same buffer, nothing special needs to be done
                checkState(this.dataBuf == nextDataBuf);
                this.prevDataNextPointerOffset = writtenEntryIndex;
            } else { //switching to a new buffer and position, flush the old one out first
                checkState(this.dataBuf != nextDataBuf);
                this.flush();

                this.dataTargetOffset = nextDataTargetOffset;
                this.dataBuf = nextDataBuf;
                this.prevDataNextPointerOffset = writtenEntryIndex;
            }
        }

        @Override
        public synchronized void flush() throws IOException {
            if (this.dataBuf != null) {
                checkState(this.dataBuf.isReadable() && this.dataTargetOffset >= 0L);
                Utils.writeFully(UInt64ToBlobMapUnsortedWriteAccess.this.dataChannel, this.dataTargetOffset, this.dataBuf);
                this.dataBuf.release();

                this.dataBuf = null;
                this.dataTargetOffset = -1L;
                this.prevDataNextPointerOffset = -1;
            }
        }

        @Override
        public synchronized void close() throws IOException {
            this.flush();
        }
    }*/
}
