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
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.ToString;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.math.PMath;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.refcount.AbstractRefCounted;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.common.util.exception.AlreadyReleasedException;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.IterableThreadLocal;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

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
    private final LongAdder writtenKeys = new LongAdder();
    private final LongAdder valueCount = new LongAdder();
    private final LongAdder valueSize = new LongAdder();

    private final Logger logger;

    private final double compressionRatio;
    private final long flushTriggerThreshold;

    private final int threads;
    private final IterableThreadLocal<ThreadState> threadStates = IterableThreadLocal.of(ThreadState::new);

    private volatile FlushInfo lastFlush;
    private volatile FlushInfo pendingFlush;

    private final Options options;
    private final List<CompletableFuture<Handle<Path>>> flushes = Collections.synchronizedList(new ArrayList<>());

    private static final Set<Thread> ONGOING_FLUSHES_GLOBAL = ConcurrentHashMap.newKeySet(PorkUtil.CPU_COUNT << 1);
    private final AtomicInteger ongoingFlushesLocal = new AtomicInteger();

    private volatile boolean flushing = false;
    private volatile boolean flushException = false;

    public OSMDataUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ToIntFunction<ByteBuffer> versionFromValueExtractor, double compressionRatio, int threads) throws Exception {
        this.storage = storage;
        this.columnFamilyHandle = columnFamilyHandle;
        this.versionFromValueExtractor = versionFromValueExtractor;
        this.compressionRatio = compressionRatio;
        this.threads = threads;

        this.options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions());

        this.logger = Logging.logger.channel(new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8));

        this.indexAddr = Memory.mmap(0L, this.indexSize, 0, 0L, Memory.MapProtection.READ_WRITE, Memory.MapVisibility.PRIVATE, Memory.MapFlags.ANONYMOUS, Memory.MapFlags.NORESERVE);

        this.lastFlush = FlushInfo.builder().valueSize(0L).targetKeyExclusive(0L).build();
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

        if (this.flushException) {
            while (true) {
                this.flushes.forEach(f -> {
                    if (f.isCompletedExceptionally()) {
                        f.join();
                    }
                });
            }
        }

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        FlushInfo lastFlush = this.lastFlush;
        checkArg(realKey >= lastFlush.targetKeyExclusive,
                "tried to put key %d, which has already been flushed (last flush was up to and excluding key %d)", realKey, lastFlush.targetKeyExclusive);

        //allocate space for the value
        long valueAddr = Memory.malloc(value.remaining() + 4L + 4L);
        PUnsafe.putUnalignedInt(valueAddr, this.versionFromValueExtractor.applyAsInt(value));
        PUnsafe.putUnalignedInt(valueAddr + 4L, value.remaining());
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
            currentKey = PMath.roundUp(currentKey, PUnsafe.pageSize() / 16L);
            this.logger.debug("scheduling flush from key %d @ %d B (inclusive) to key %d @ %d B (exclusive)", this.lastFlush.targetKeyExclusive, this.lastFlush.valueSize, currentKey, valueSize);
            this.pendingFlush = FlushInfo.builder().valueSize(valueSize).targetKeyExclusive(currentKey).build();
        }
    }

    private void visitKey(long currentKey) {
        checkArg(currentKey >= 0L);

        this.maxKey.accumulate(currentKey);

        ThreadState state = this.threadStates.get();
        checkState(state.state == ThreadState.State.JOINED);

        long lastKey = state.lastKey;
        state.lastKey = currentKey;

        /*FlushInfo pendingFlush = this.pendingFlush;
        if (pendingFlush != null) {
            if (lastKey < pendingFlush.targetKeyExclusive && currentKey >= pendingFlush.targetKeyExclusive) {
                //this thread has advanced to its first key beyond the target key
                this.tryTriggerPendingFlush(pendingFlush);
            }
        }*/
    }

    private void tryTriggerPendingFlush(@NonNull FlushInfo pendingFlush) {
        List<ThreadState> otherThreadsStates = this.threadStates.snapshotValues();
        if (otherThreadsStates.size() == this.threads && otherThreadsStates.stream().allMatch(otherState -> otherState.state == ThreadState.State.ACTIVELY_FLUSHING
                                                                                                            || otherState.state == ThreadState.State.QUIT
                                                                                                            || otherState.lastKey > pendingFlush.targetKeyExclusive
                                                                                                            || ONGOING_FLUSHES_GLOBAL.contains(otherState.thread))) {
            //every other thread has already been started and has passed the target key
            this.executePendingFlush(pendingFlush);
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

        int ongoingFlushesLocal = this.ongoingFlushesLocal.incrementAndGet();
        ONGOING_FLUSHES_GLOBAL.add(Thread.currentThread());

        this.logger.debug("executing flush: %s (%d flushes active, %d across all column families)", pendingFlush, ongoingFlushesLocal, ONGOING_FLUSHES_GLOBAL.size());

        /*this.flushes.add(CompletableFuture.supplyAsync((ESupplier<Handle<Path>>) () -> this.buildSstFileFromRange(
                this.indexAddr + (lastFlush.targetKey + 1L) * 16L,
                (pendingFlush.targetKey - lastFlush.targetKey) * 16L)));*/

        CompletableFuture<Handle<Path>> flushFuture = new CompletableFuture<>();
        this.flushes.add(flushFuture);
        try {
            flushFuture.complete(this.buildSstFileFromRange(
                    this.indexAddr + lastFlush.targetKeyExclusive * 16L,
                    (pendingFlush.targetKeyExclusive - lastFlush.targetKeyExclusive) * 16L));
        } catch (Throwable t) {
            flushFuture.completeExceptionally(t);
            throw PUnsafe.throwException(t);
        } finally {
            ONGOING_FLUSHES_GLOBAL.remove(Thread.currentThread());
            this.ongoingFlushesLocal.decrementAndGet();
        }
    }

    public void threadJoin() {
        ThreadState state = this.threadStates.get();
        checkState(state.state == ThreadState.State.INACTIVE, state.state);
        state.state = ThreadState.State.JOINED;
    }

    public void threadRemove() {
        ThreadState state = this.threadStates.get();
        checkState(state.state == ThreadState.State.JOINED, state.state);

        try {
            FlushInfo pendingFlush = this.pendingFlush;
            if (pendingFlush != null && state.lastKey >= pendingFlush.targetKeyExclusive) {
                state.state = ThreadState.State.ACTIVELY_FLUSHING;
                this.tryTriggerPendingFlush(pendingFlush);
            }
        } finally {
            state.state = ThreadState.State.INACTIVE;
        }
    }

    public void threadQuit() {
        ThreadState state = this.threadStates.get();
        checkState(state.state == ThreadState.State.INACTIVE, state.state);
        state.state = ThreadState.State.QUIT;
    }

    @Override
    public synchronized void flush() throws Exception { //this is actually only allowed to be called once
        checkState(!this.flushing, "already flushing?!?");
        this.flushing = true;

        checkState(this.threadStates.snapshotValues().stream().allMatch(state -> !state.thread().isAlive() || state.state == ThreadState.State.QUIT), "some worker threads are still alive?!?");

        if (this.pendingFlush != null) {
            this.executePendingFlush(this.pendingFlush);
        }

        long lastKey = this.maxKey.get();
        this.scheduleFlush(lastKey, this.valueSize.sum());
        this.executePendingFlush(this.pendingFlush);
    }

    private static native int trySwapIndexEntry(long indexBegin, long key, long value);

    private static native long appendKeys(long writerHandle, long begin, long end);

    @SneakyThrows
    private Handle<Path> buildSstFileFromRange(long indexAddr, long indexSize) throws Exception {
        checkState(indexAddr % PUnsafe.pageSize() == 0L, indexAddr);
        checkState(indexSize % PUnsafe.pageSize() == 0L, indexSize);
        Memory.mprotect(indexAddr, indexSize, Memory.MapProtection.READ);

        try {
            checkArg(indexSize % 16L == 0L, indexSize);

            if (indexSize == 0L) {
                this.logger.warn("attempted to build empty sst file!");
                return null;
            }

            Handle<Path> pathHandle = this.storage.getTmpFilePath(new String(this.columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sst");

            try (SstFileWriter writer = new SstFileWriter(this.storage.db().config().envOptions(), this.options)) {
                writer.open(pathHandle.get().toString());
                long writtenKeys = appendKeys(writer.getNativeHandle(), indexAddr, indexAddr + indexSize);
                if (writtenKeys == 0L) {
                    //no keys were appended
                    PFiles.rm(pathHandle.get());
                    pathHandle.close();
                    return null;
                }
                this.writtenKeys.add(writtenKeys);

                writer.finish();
            }

            return pathHandle;
        } catch (Throwable t) {
            this.flushException = true;
            throw t;
        } finally {
            Memory.madvise(indexAddr, indexSize, Memory.Usage.MADV_DONTNEED);
            Memory.mprotect(indexAddr, indexSize, Memory.MapProtection.NONE);
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
            Memory.madvise(this.indexAddr, this.indexSize, Memory.Usage.MADV_DONTNEED);
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

        this.logger.success("ingested %d keys (valueCount reports %d) totalling %d bytes (%.2f MiB) in %d SST files totalling %d bytes (%.2f MiB)",
                this.writtenKeys.sum(), this.valueCount.sum(),
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
        private final long targetKeyExclusive;
    }

    @Getter
    private static final class ThreadState {
        private final Thread thread = Thread.currentThread();
        private State state = State.INACTIVE;

        private long lastKey = 0L;

        enum State {
            INACTIVE,
            JOINED,
            ACTIVELY_FLUSHING,
            QUIT,
        }
    }

    @Getter
    private final class SequentialAllocationArena extends AbstractRefCounted {
        private static final long MAX_SIZE = 1L << 30L; // 1GiB

        private final long addr;
        private long size;

        private long nextAlloc;

        private boolean locked;

        public SequentialAllocationArena() {
            this.addr = Memory.mmap(0L, MAX_SIZE, 0, 0L, Memory.MapProtection.READ_WRITE, Memory.MapVisibility.PRIVATE, Memory.MapFlags.ANONYMOUS, Memory.MapFlags.NORESERVE);
            this.size = MAX_SIZE;
        }

        public synchronized long alloc(long len) {
            checkState(!this.locked);
            if (this.nextAlloc + notNegative(len, "len") > this.size) {
                throw new OutOfMemoryError(String.valueOf(len));
            }
            long addr = this.addr + this.nextAlloc;
            this.nextAlloc += len;
            return addr;
        }

        public synchronized void lock() {
            checkState(!this.locked);
            this.locked = true;

            //shrink the mapping down to the minimum size
            long trimmedSize = PMath.roundUp(this.nextAlloc, PUnsafe.pageSize());
            long newAddr = Memory.mremap(this.addr, this.size, trimmedSize);
            checkState(this.addr == newAddr, "mremap returned a different address!");
            this.size = trimmedSize;

            //make the data read-only
            Memory.mprotect(this.addr, this.size, Memory.MapProtection.READ);
        }

        @Override
        public SequentialAllocationArena retain() throws AlreadyReleasedException {
            super.retain();
            return this;
        }

        @Override
        protected void doRelease() {
            Memory.munmap(this.addr, this.size);
        }
    }
}
