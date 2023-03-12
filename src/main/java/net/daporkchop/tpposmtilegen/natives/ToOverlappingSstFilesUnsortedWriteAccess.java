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

import lombok.NonNull;
import lombok.SneakyThrows;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.util.IterableThreadLocal;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
public final class ToOverlappingSstFilesUnsortedWriteAccess extends AbstractUnsortedWriteAccess {
    private final LongAdder totalDataSize = new LongAdder();

    private final long flushTriggerThreshold;

    private final IterableThreadLocal<WriteBuffer> threadStates = IterableThreadLocal.of(WriteBuffer::new);
    private final List<Handle<Path>> sstPaths = Collections.synchronizedList(new ArrayList<>());

    private final Options options;

    public ToOverlappingSstFilesUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle, double compressionRatio) throws Exception {
        super(storage, columnFamilyHandle);

        this.options = new Options(this.storage.db().config().dbOptions(), this.storage.db().columns().get(this.columnFamilyHandle).getOptions());

        this.flushTriggerThreshold = (long) (compressionRatio * this.options.targetFileSizeBase());
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        this.threadStates.get().put(key, value);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        this.threadStates.get().put(key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        this.threadStates.get().merge(key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        this.threadStates.get().merge(key, value);
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        this.threadStates.get().delete(key);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.totalDataSize.sum();
    }

    @Override
    protected void flush0() throws Exception {
        this.threadStates.forEach(WriteBuffer::flush);

        int totalCount = this.sstPaths.size();
        long totalSize = this.sstPaths.stream().map(Handle::get).map((IOFunction<Path, Long>) Files::size).mapToLong(Long::longValue).sum();

        if (totalCount != 0) {
            //ingest the SST files
            try (TimedOperation ingestOperation = new TimedOperation("Ingest SST files", this.logger)) {
                this.storage.db().delegate().ingestExternalFile(this.columnFamilyHandle,
                        this.sstPaths.stream().map(Handle::get).map(Path::toString).collect(Collectors.toList()),
                        this.storage.db().config().ingestOptions(DatabaseConfig.IngestType.MOVE));
                this.sstPaths.forEach(Handle::release);
                this.sstPaths.clear();
            }
        }

        this.logger.success("ingested %d bytes (%.2f MiB) in %d SST files totalling %d bytes (%.2f MiB)",
                this.getDataSize(), this.getDataSize() / (1024.0d * 1024.0d),
                totalCount, totalSize, totalSize / (1024.0d * 1024.0d));
    }

    @Override
    public synchronized void close() throws Exception {
        this.flush();
        this.threadStates.forEach(WriteBuffer::close);

        this.options.close();

        Memory.releaseMemoryToSystem();
    }

    private static final long ENTRY_OVERHEAD = 16L;

    private static native long createState0(long optionsHandle);

    private static native void deleteState0(long state);

    private static native void put0(long state, long keyAddr, int keySize, long valueAddr, int valueSize);

    private static native void merge0(long state, long keyAddr, int keySize, long valueAddr, int valueSize);

    private static native void delete0(long state, long keyAddr, int keySize);

    private static native void clear0(long state);

    private static native void flush0(long state, long writerHandle);

    /**
     * @author DaPorkchop_
     */
    private final class WriteBuffer implements Flushable, Closeable {
        private final long state = createState0(ToOverlappingSstFilesUnsortedWriteAccess.this.options.getNativeHandle());

        private long currentDataSize = 0L;
        private long currentWrittenCount = 0L;

        public void put(@NonNull byte[] key, @NonNull byte[] value) {
            long allocationSize = (long) key.length + value.length;
            long addr = Memory.malloc(allocationSize);
            try {
                Memory.memcpy(addr + 0L, key, 0, key.length);
                Memory.memcpy(addr + key.length, value, 0, value.length);
                this.put(addr + 0L, key.length, addr + key.length, value.length);
            } finally {
                Memory.free(addr, allocationSize);
            }
        }

        public void put(@NonNull ByteBuffer key, @NonNull ByteBuffer value) {
            this.put(
                    PUnsafe.pork_directBufferAddress(key) + key.position(), key.remaining(),
                    PUnsafe.pork_directBufferAddress(value) + value.position(), value.remaining());
        }

        private synchronized void put(long keyAddr, int keySize, long valueAddr, int valueSize) {
            put0(this.state, keyAddr, keySize, valueAddr, valueSize);
            this.postWrite(keySize, valueSize);
        }

        public void merge(@NonNull byte[] key, @NonNull byte[] value) {
            long allocationSize = (long) key.length + value.length;
            long addr = Memory.malloc(allocationSize);
            try {
                Memory.memcpy(addr + 0L, key, 0, key.length);
                Memory.memcpy(addr + key.length, value, 0, value.length);
                this.merge(addr + 0L, key.length, addr + key.length, value.length);
            } finally {
                Memory.free(addr, allocationSize);
            }
        }

        public void merge(@NonNull ByteBuffer key, @NonNull ByteBuffer value) {
            this.merge(
                    PUnsafe.pork_directBufferAddress(key) + key.position(), key.remaining(),
                    PUnsafe.pork_directBufferAddress(value) + value.position(), value.remaining());
        }

        private synchronized void merge(long keyAddr, int keySize, long valueAddr, int valueSize) {
            merge0(this.state, keyAddr, keySize, valueAddr, valueSize);
            this.postWrite(keySize, valueSize);
        }

        public void delete(@NonNull byte[] key) {
            long addr = Memory.malloc(key.length);
            try {
                Memory.memcpy(addr, key, 0, key.length);
                this.delete(addr, key.length);
            } finally {
                Memory.free(addr, key.length);
            }
        }

        public void delete(@NonNull ByteBuffer key) {
            this.delete(PUnsafe.pork_directBufferAddress(key) + key.position(), key.remaining());
        }

        private synchronized void delete(long keyAddr, int keySize) {
            delete0(this.state, keyAddr, keySize);
            this.postWrite(keySize, 0);
        }

        private void postWrite(int keySize, int valueSize) {
            long totalSize = ENTRY_OVERHEAD + keySize + valueSize;

            ToOverlappingSstFilesUnsortedWriteAccess.this.totalDataSize.add(totalSize);
            this.currentDataSize += totalSize;
            this.currentWrittenCount++;

            if (this.currentDataSize >= ToOverlappingSstFilesUnsortedWriteAccess.this.flushTriggerThreshold) {
                this.flush();
            }
        }

        @Override
        @SneakyThrows({ IOException.class, RocksDBException.class })
        public synchronized void flush() {
            if (this.currentWrittenCount != 0L) {
                this.currentWrittenCount = 0L;
                this.currentDataSize = 0L;

                Storage storage = ToOverlappingSstFilesUnsortedWriteAccess.this.storage;

                Handle<Path> pathHandle = storage.getTmpFilePath(ToOverlappingSstFilesUnsortedWriteAccess.this.columnFamilyName, "sst");
                try (SstFileWriter writer = new SstFileWriter(storage.db().config().envOptions(), ToOverlappingSstFilesUnsortedWriteAccess.this.options)) {
                    writer.open(pathHandle.get().toString());
                    flush0(this.state, writer.getNativeHandle());
                    clear0(this.state); //should be unnecessary, but just in case
                    writer.finish();
                } catch (Throwable t) {
                    PFiles.rm(pathHandle.get());
                    pathHandle.release();
                    throw PUnsafe.throwException(t);
                }
                ToOverlappingSstFilesUnsortedWriteAccess.this.sstPaths.add(pathHandle);
            }
        }

        @Override
        public synchronized void close() {
            this.flush();
            deleteState0(this.state);
        }
    }
}
