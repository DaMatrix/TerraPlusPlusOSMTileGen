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

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.ToString;
import net.daporkchop.lib.common.function.exception.ESupplier;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.math.PMath;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.IterableThreadLocal;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class OSMDataUnsortedWriteAccess implements DBWriteAccess {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
        init();
    }

    private static native void init();

    private final Storage storage;
    private final ColumnFamilyHandle columnFamilyHandle;
    private final ToIntFunction<ByteBuffer> versionFromValueExtractor;

    private final long indexAddr;
    private final long indexSize = 1L << 40L;

    private final LongAccumulator maxKey = new LongAccumulator(Long::max, 0L);
    private final LongAdder valueCount = new LongAdder();
    private final LongAdder valueSize = new LongAdder();

    private final Logger logger;

    private final double compressionRatio;
    private final long flushTriggerThreshold;

    private final int threads;
    private final IterableThreadLocal<MutableLong> lastKeyPerThread = IterableThreadLocal.of(MutableLong::new);

    private volatile FlushInfo lastFlush;
    private volatile FlushInfo pendingFlush;

    private final Options options;
    private final List<CompletableFuture<Handle<Path>>> flushes = Collections.synchronizedList(new ArrayList<>());

    private final Semaphore ongoingFlushes = new Semaphore(0);

    private volatile boolean flushing = false;
    private volatile boolean flushException = false;

    public OSMDataUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ToIntFunction<ByteBuffer> versionFromValueExtractor, double compressionRatio, int threads) throws Exception {
        this.storage = storage;
        this.columnFamilyHandle = columnFamilyHandle;
        this.versionFromValueExtractor = versionFromValueExtractor;
        this.compressionRatio = compressionRatio;
        this.threads = threads;

        this.ongoingFlushes.release(threads);

        this.options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions());

        this.logger = Logging.logger.channel(new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8));

        this.indexAddr = Memory.mmapAnon(this.indexSize);

        this.lastFlush = FlushInfo.builder().valueSize(0L).targetKey(0L).build();
        this.flushTriggerThreshold = (long) (compressionRatio * this.options.targetFileSizeBase());

        Memory.releaseMemoryToSystem();
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

        if (this.ongoingFlushes.availablePermits() == 0) { //the work queue is full
            this.logger.debug("work queue is full, blocking...");
            this.ongoingFlushes.acquireUninterruptibly();
            this.ongoingFlushes.release();
        }

        if (this.flushException) {
            this.flushes.forEach(f -> {
                if (f.isCompletedExceptionally()) {
                    f.join();
                }
            });
        }

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        //allocate space for the value
        long valueAddr = Memory.malloc(value.remaining() + 4L + 4L);
        PUnsafe.putUnalignedIntLE(valueAddr, this.versionFromValueExtractor.applyAsInt(value));
        PUnsafe.putUnalignedIntLE(valueAddr + 4L, value.remaining());
        PUnsafe.copyMemory(PUnsafe.pork_directBufferAddress(value) + value.position(), valueAddr + 8L, value.remaining());

        int oldSize = trySwapIndexEntry(this.indexAddr, realKey, valueAddr);
        if (oldSize >= 0) { //we successfully replaced the previous value
            long d = value.remaining() + 4L + 4L;
            if (oldSize > 0) { //a previous value existed
                d -= oldSize + 4L + 4L;
            } else {
                this.valueCount.increment();
            }
            this.valueSize.add(d);

            long valueSize;
            if (this.pendingFlush == null && (valueSize = this.valueSize.sum()) - this.lastFlush.valueSize >= this.flushTriggerThreshold) {
                this.scheduleFlush(realKey, valueSize);
            }
        }

        this.visitKey(realKey);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
        throw new UnsupportedOperationException();
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
        return this.valueSize.sum();
    }

    private synchronized void scheduleFlush(long currentKey, long valueSize) {
        if (this.pendingFlush == null) {
            this.logger.debug("scheduling flush from key %d @ %d B to key %d @ %d B", this.lastFlush.targetKey, this.lastFlush.valueSize, currentKey, valueSize);
            this.pendingFlush = FlushInfo.builder().valueSize(valueSize).targetKey(currentKey).build();
        }
    }

    private void visitKey(long currentKey) {
        checkArg(currentKey >= 0L);

        this.maxKey.accumulate(currentKey);

        MutableLong lastKeyPerThread = this.lastKeyPerThread.get();
        long lastKey = lastKeyPerThread.longValue();
        lastKeyPerThread.setValue(currentKey);

        FlushInfo pendingFlush = this.pendingFlush;
        if (pendingFlush != null) {
            if (lastKey <= pendingFlush.targetKey && currentKey > pendingFlush.targetKey) {
                //this thread has advanced to its first key beyond the target key
                List<MutableLong> otherThreadsLastKey = this.lastKeyPerThread.snapshotValues();
                checkState(otherThreadsLastKey.size() <= this.threads, otherThreadsLastKey.size());
                if (otherThreadsLastKey.size() == this.threads && otherThreadsLastKey.stream().allMatch(l -> l.longValue() >= pendingFlush.targetKey)) {
                    //every other thread has already been started and has passed the target key
                    this.executePendingFlush(pendingFlush);
                } else {
                    int i = 0;
                }
            }
        }
    }

    private void executePendingFlush(@NonNull FlushInfo pendingFlush) {

        FlushInfo lastFlush;
        synchronized (this) {
            if (this.pendingFlush != pendingFlush) {
                this.logger.debug("not executing flush " + pendingFlush + " as it's already been started by another thread");
                return;
            }

            lastFlush = this.lastFlush;
            this.lastFlush = pendingFlush;
            this.pendingFlush = null;
        }

        this.logger.debug("executing flush: " + pendingFlush + " (" + (this.threads - this.ongoingFlushes.availablePermits()) + " flushes active)");

        this.flushes.add(CompletableFuture.supplyAsync((ESupplier<Handle<Path>>) () -> this.buildSstFileFromRange(
                this.indexAddr + (lastFlush.targetKey + 1L) * 16L,
                (pendingFlush.targetKey - lastFlush.targetKey) * 16L)));

        /*CompletableFuture<Handle<Path>> flushFuture = new CompletableFuture<>();
        this.flushes.add(flushFuture);
        try {
            flushFuture.complete(this.buildSstFileFromRange(
                    this.indexMapping.addr() + (lastFlush.targetKey + 1L) * 16L,
                    (pendingFlush.targetKey - lastFlush.targetKey) * 16L));
        } catch (Throwable t) {
            flushFuture.completeExceptionally(t);
            throw PUnsafe.throwException(t);
        }*/
    }

    @Override
    public synchronized void flush() throws Exception { //this is actually only allowed to be called once
        checkState(!this.flushing, "already flushing?!?");
        this.flushing = true;

        checkState(this.lastKeyPerThread.snapshotValues().isEmpty(), "some worker threads are still alive?!?");

        if (this.pendingFlush != null) {
            this.executePendingFlush(this.pendingFlush);
        }

        //long lastKey = this.lastKeyPerThread.snapshotValues().stream().mapToLong(MutableLong::longValue).max().getAsLong();
        long lastKey = this.maxKey.get();
        this.scheduleFlush(lastKey, this.valueSize.sum());
        this.executePendingFlush(this.pendingFlush);
    }

    private static native long[] partitionSortedRange(long addr, long size, long targetBlockSize) throws OutOfMemoryError;

    private static native int trySwapIndexEntry(long indexBegin, long key, long valueOffset);

    private static native boolean appendKeys(long writerHandle, long begin, long end);

    @SneakyThrows
    private Handle<Path> buildSstFileFromRange(long indexAddr, long indexSize) throws Exception {
        this.ongoingFlushes.acquireUninterruptibly();
        try {
            checkArg(indexSize % 16L == 0L, indexSize);

            if (indexSize == 0L) {
                this.logger.warn("attempted to build empty sst file!");
                return null;
            }

            Handle<Path> pathHandle = this.storage.getTmpFilePath(new String(this.columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sst");

            try (SstFileWriter writer = new SstFileWriter(this.storage.db().config().envOptions(), this.options)) {
                writer.open(pathHandle.get().toString());
                if (!appendKeys(writer.getNativeHandle(), indexAddr, indexAddr + indexSize)) {
                    //no keys were appended
                    PFiles.rm(pathHandle.get());
                    pathHandle.close();
                    return null;
                }
                writer.finish();
            } finally {
                long start = PMath.roundUp(indexAddr, PUnsafe.pageSize());
                long size = PMath.roundUp(indexSize - 3L * PUnsafe.pageSize(), PUnsafe.pageSize());
                if (size > 0L) {
                    checkRangeLen(indexAddr, indexAddr + indexSize, start, size);
                    Memory.madvise(start, size, Memory.Usage.MADV_REMOVE);
                }
            }

            return pathHandle;
        } catch (Throwable t) {
            this.flushException = true;
            throw t;
        } finally {
            this.ongoingFlushes.release();
        }
    }

    @Override
    public synchronized void clear() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void close() throws Exception {
        if (!this.flushing) {
            this.flush();
        }

        List<Handle<Path>> paths;
        try {
            paths = this.flushes.stream().map(CompletableFuture::join).collect(Collectors.toList());
        } catch (Exception e) {
            while (!this.flushes.stream().allMatch(CompletableFuture::isDone)) {
                try {
                    this.flushes.stream().filter(f -> !f.isDone()).forEach(CompletableFuture::join);
                } catch (Exception e1) {
                    e1.addSuppressed(e);
                }
            }
            throw e;
        } finally {
            Memory.madvise(this.indexAddr, this.indexSize, Memory.Usage.MADV_NORMAL);
        }

        paths.removeIf(Objects::isNull);
        int totalCount = paths.size();
        long totalSize = paths.stream().map(Handle::get).map((IOFunction<Path, Long>) Files::size).mapToLong(Long::longValue).sum();

        if (totalCount != 0) {
            //ingest the SST files
            try (TimedOperation ingestOperation = new TimedOperation("Ingest SST files", this.logger)) {
                this.storage.db().delegate().ingestExternalFile(this.columnFamilyHandle,
                        paths.stream().map(Handle::get).map(Path::toString).collect(Collectors.toList()),
                        this.storage.db().config().ingestOptions(DatabaseConfig.IngestType.MOVE));
                paths.forEach(Handle::release);
                paths.clear();
            }
        }

        this.logger.success("ingested %d bytes (%.2f MiB) in %d SST files totalling %d bytes (%.2f MiB)",
                this.getDataSize(), this.getDataSize() / (1024.0d * 1024.0d),
                totalCount, totalSize, totalSize / (1024.0d * 1024.0d));

        Memory.munmap(this.indexAddr, this.indexSize);

        this.options.close();

        Memory.releaseMemoryToSystem();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }

    @Builder
    @ToString
    private static final class FlushInfo {
        private final long valueSize;
        private final long targetKey;
    }
}
