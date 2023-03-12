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

import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.exception.ESupplier;
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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final LongObjMap<TileState> keysToStates = new LongObjConcurrentHashMap<>();

    private final Handle<Path> dataPathHandle;
    private final FileChannel dataChannel;
    private final MemoryMap dataMmap;
    private final long dataSize = 1L << 40L;

    private final AtomicLong dataAddrAllocator = new AtomicLong();

    private final double compressionRatio;

    public UInt64ToBlobMapUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, double compressionRatio) throws Exception {
        super(storage, columnFamilyHandle);
        this.compressionRatio = compressionRatio;

        this.dataPathHandle = storage.getTmpFilePath(
                UInt64ToBlobMapUnsortedWriteAccess.class.getSimpleName() + '-' + new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), "buf");
        this.dataChannel = FileChannel.open(this.dataPathHandle.get(), READ, WRITE, CREATE_NEW);
        Utils.truncate(this.dataChannel, this.dataSize);
        this.dataMmap = new MemoryMap(this.dataChannel, FileChannel.MapMode.READ_WRITE, 0L, this.dataSize);

        Memory.madvise(this.dataMmap.addr(), this.dataSize, Memory.Usage.MADV_SEQUENTIAL);

        this.dataAddrAllocator.set(this.dataMmap.addr() + this.dataSize);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        UInt64ToBlobMapMergeOperator.decodeToSlices(Unpooled.wrappedBuffer(value), (entryKey, data) -> {
            int dataSize = data.readableBytes();
            long fullDataSize = 8L + 8L + 4L + dataSize;

            long valueAddr = this.dataAddrAllocator.addAndGet(-fullDataSize);
            checkState(valueAddr >= this.dataMmap.addr());

            PUnsafe.putUnalignedLong(valueAddr, 0L);
            PUnsafe.putUnalignedLong(valueAddr + 8L, entryKey);
            PUnsafe.putUnalignedInt(valueAddr + 8L + 8L, dataSize);
            Memory.memcpy(valueAddr + 8L + 8L + 4L, data.memoryAddress() + data.readerIndex(), dataSize);

            TileState state = this.keysToStates.computeIfAbsent(realKey, TileState::new);
            synchronized (state) {
                //new.next = old;
                checkState(PUnsafe.compareAndSwapLong(null, valueAddr, 0L, state.prevPtr));
                state.prevPtr = valueAddr;

                state.dataSize += fullDataSize;
            }
        });
    }

    @Override
    public long getDataSize() throws Exception {
        return this.dataAddrAllocator.get() - this.dataMmap.addr();
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
            List<List<TileState>> batches = new ArrayList<>();

            {
                List<TileState> currentBatch = null;
                long currentBatchSize = 0L;

                TileState[] allTileStates = this.keysToStates.values().toArray(new TileState[0]);
                this.keysToStates.clear();
                Arrays.parallelSort(allTileStates, (a, b) -> Long.compareUnsigned(a.key, b.key));

                for (TileState entry : allTileStates) {
                    if (currentBatch == null) {
                        currentBatch = new ArrayList<>();
                        batches.add(currentBatch);
                    }

                    currentBatch.add(entry);
                    currentBatchSize += entry.dataSize;

                    if (currentBatchSize >= targetBlockSize) {
                        currentBatch = null;
                        currentBatchSize = 0L;
                    }
                }
            }

            Memory.madvise(this.dataMmap.addr(), this.dataSize, Memory.Usage.MADV_WILLNEED);

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

        CompletableFuture<?> removeFuture = CompletableFuture.runAsync(() -> Memory.madvise(this.dataMmap.addr(), this.dataSize, Memory.Usage.MADV_REMOVE));

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

        removeFuture.join();
    }

    private static native long appendKey(long writerHandle, long key, long root);

    private Handle<Path> buildSstFileFromRange(@NonNull EnvOptions envOptions, @NonNull Options options, @NonNull List<TileState> batch, @NonNull LongAdder reportedTotalSize) throws Exception {
        checkArg(!batch.isEmpty());

        Handle<Path> pathHandle = this.storage.getTmpFilePath(this.columnFamilyName, "sst");

        try (SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(pathHandle.get().toString());

            for (TileState entry : batch) {
                long writtenData = appendKey(writer.getNativeHandle(), entry.key, entry.prevPtr);
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
    private static class TileState {
        private final long key;
        private long prevPtr = 0L;
        private long dataSize = 0L;
    }
}
