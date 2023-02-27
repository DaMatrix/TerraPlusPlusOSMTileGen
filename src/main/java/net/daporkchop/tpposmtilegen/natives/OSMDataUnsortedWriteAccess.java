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
import net.daporkchop.tpposmtilegen.util.mmap.alloc.dynamic.SequentialDynamicAllocator;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.sparse.SequentialSparseAllocator;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;
import static net.daporkchop.lib.logging.Logging.*;

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

    private final Handle<Path> indexPathHandle;
    private final FileChannel indexChannel;
    private final MemoryMap indexMapping;

    private final LongAccumulator maxIndexId = new LongAccumulator(Long::max, 0L);
    private final LongAdder valueCount = new LongAdder();
    private final LongAdder valueSize = new LongAdder();

    private final Handle<Path> dataPathHandle;
    private final SequentialSparseAllocator dataMapping;

    private final double compressionRatio;

    private volatile boolean flushing = false;

    public OSMDataUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ToIntFunction<ByteBuffer> versionFromValueExtractor, double compressionRatio) throws Exception {
        this.storage = storage;
        this.columnFamilyHandle = columnFamilyHandle;
        this.versionFromValueExtractor = versionFromValueExtractor;
        this.compressionRatio = compressionRatio;

        this.indexPathHandle = storage.getTmpFilePath(
                OSMDataUnsortedWriteAccess.class.getSimpleName() + '-' + new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), "index");
        this.indexChannel = FileChannel.open(this.indexPathHandle.get(), READ, WRITE, CREATE_NEW, SPARSE);
        MemoryMap.truncate0(this.indexChannel, 1L << 40L);
        checkState(this.indexChannel.size() == (1L << 40L));
        this.indexMapping = new MemoryMap(this.indexChannel, FileChannel.MapMode.READ_WRITE, 0L, this.indexChannel.size());
        Memory.madvise(this.indexMapping.addr(), this.indexMapping.size(), Memory.Usage.MADV_REMOVE);

        this.dataPathHandle = storage.getTmpFilePath(
                OSMDataUnsortedWriteAccess.class.getSimpleName() + '-' + new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), "data");
        this.dataMapping = new SequentialSparseAllocator(this.dataPathHandle.get(), 1L << 40L);
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

        checkArg(key.remaining() == 8, key.remaining());
        long realKey = PUnsafe.getUnalignedLongBE(PUnsafe.pork_directBufferAddress(key) + key.position());

        this.maxIndexId.accumulate(realKey);

        //allocate space for the value
        long valueAddr = this.dataMapping.addr() + this.dataMapping.alloc(value.remaining() + 4L + 4L);
        PUnsafe.putUnalignedIntLE(valueAddr, this.versionFromValueExtractor.applyAsInt(value));
        PUnsafe.putUnalignedIntLE(valueAddr + 4L, value.remaining());
        PUnsafe.copyMemory(PUnsafe.pork_directBufferAddress(value) + value.position(), valueAddr + 8L, value.remaining());

        //logger.info("putting entry with key %d of size %d", realKey, value.remaining());

        int oldSize = trySwapIndexEntry(this.indexMapping.addr(), realKey, valueAddr);
        if (oldSize >= 0) { //we successfully replaced the previous value
            long d = value.remaining() + 4L + 4L;
            if (oldSize > 0) { //a previous value existed
                d -= oldSize + 4L + 4L;
            } else {
                this.valueCount.increment();
            }
            this.valueSize.add(d);
        }
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

    @Override
    public synchronized void flush() throws Exception {
        this.flushing = true;

        final long highestIndexId = this.maxIndexId.getThenReset();

        final long indexBeginAddr = this.indexMapping.addr();
        final long indexEndAddr = this.indexMapping.addr() + ((highestIndexId + 1L) * 16L);

        final long dataBeginAddr = this.dataMapping.addr();
        final long dataEndAddr = this.dataMapping.addr() + this.dataMapping.size();

        final long valueSize = this.valueSize.sumThenReset();
        final long valueCount = this.valueCount.sumThenReset();
        final double averageEntrySize = (double) valueSize / valueCount;
        final double averageEntryDensity = (double) (indexEndAddr - indexBeginAddr) / valueCount;

        //split the buffer's contents and build SST files out of it
        List<Handle<Path>> paths;
        try (TimedOperation sstOperation = new TimedOperation("Build SST files");
             Options options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions())) {
            EnvOptions envOptions = this.storage.db().config().envOptions();

            long targetBlockSize = PMath.roundUp((long) (this.compressionRatio * options.targetFileSizeBase() / averageEntrySize * averageEntryDensity), 16L);
            long[] blockStartAddrs = partitionSortedRange(indexBeginAddr, indexEndAddr - indexBeginAddr, targetBlockSize);

            Memory.madvise(indexBeginAddr, indexEndAddr - indexBeginAddr, Memory.Usage.MADV_SEQUENTIAL);
            Memory.madvise(dataBeginAddr, dataEndAddr - dataBeginAddr, Memory.Usage.MADV_SEQUENTIAL);

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
        } finally {
            Memory.madvise(indexBeginAddr, indexEndAddr - indexBeginAddr, Memory.Usage.MADV_NORMAL);
            Memory.madvise(dataBeginAddr, dataEndAddr - dataBeginAddr, Memory.Usage.MADV_NORMAL);
        }

        this.clear();
        paths.removeIf(Objects::isNull);

        //ingest the SST files
        try (TimedOperation ingestOperation = new TimedOperation("Ingest SST files")) {
            this.storage.db().delegate().ingestExternalFile(this.columnFamilyHandle,
                    paths.stream().map(Handle::get).map(Path::toString).collect(Collectors.toList()),
                    this.storage.db().config().ingestOptions(DatabaseConfig.IngestType.MOVE));
            paths.forEach(Handle::release);
            paths.clear();
        }

        this.flushing = false;
    }

    private static native long[] partitionSortedRange(long addr, long size, long targetBlockSize) throws OutOfMemoryError;

    private static native int trySwapIndexEntry(long indexBegin, long key, long valueOffset);

    private static native boolean appendKeys(long writerHandle, long begin, long end);

    private Handle<Path> buildSstFileFromRange(@NonNull EnvOptions envOptions, @NonNull Options options, long indexAddr, long indexSize) throws Exception {
        checkArg(indexSize % 16L == 0L, indexSize);

        Handle<Path> pathHandle = this.storage.getTmpFilePath(new String(this.columnFamilyHandle.getName(), StandardCharsets.UTF_8), "sst");

        try (SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(pathHandle.get().toString());
            if (!appendKeys(writer.getNativeHandle(), indexAddr, indexAddr + indexSize)) {
                //no keys were appended
                PFiles.rm(pathHandle.get());
                pathHandle.close();
                return null;
            }
            writer.finish();
        }

        return pathHandle;
    }

    @Override
    public synchronized void clear() throws Exception {
        Memory.madvise(this.indexMapping.addr(), this.indexMapping.size(), Memory.Usage.MADV_REMOVE);
        this.dataMapping.clear();
        this.maxIndexId.reset();
        this.valueSize.reset();
        this.valueCount.reset();
    }

    @Override
    public synchronized void close() throws Exception {
        this.flush();

        this.indexMapping.close();
        this.indexChannel.close();
        PFiles.rm(this.indexPathHandle.get());
        this.indexPathHandle.release();

        this.dataMapping.close();
        PFiles.rm(this.dataPathHandle.get());
        this.dataPathHandle.release();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }
}
