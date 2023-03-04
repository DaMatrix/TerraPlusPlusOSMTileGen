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

package net.daporkchop.tpposmtilegen.storage.rocksdb;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.common.function.exception.EBiConsumer;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.function.exception.EPredicate;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.natives.Memory;
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.BulkFlushable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
class FlushableSstFileWriterBatch implements DBWriteAccess, BulkFlushable<FlushableSstFileWriterBatch> {
    protected final DatabaseConfig config;
    protected final Database database;

    protected final Map<ColumnFamilyHandle, ColumnState> statesPerColumn = new IdentityHashMap<>();

    public FlushableSstFileWriterBatch(@NonNull DatabaseConfig config, @NonNull Database database) {
        this.config = config;
        this.database = database;
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.getColumnState(columnFamilyHandle).put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.getColumnState(columnFamilyHandle).put(columnFamilyHandle, key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.getColumnState(columnFamilyHandle).merge(columnFamilyHandle, key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.getColumnState(columnFamilyHandle).merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.getColumnState(columnFamilyHandle).delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        this.getColumnState(columnFamilyHandle).deleteRange(columnFamilyHandle, beginKey, endKey);
    }

    protected ColumnState getColumnState(@NonNull ColumnFamilyHandle columnFamilyHandle) {
        ColumnState state = this.statesPerColumn.get(columnFamilyHandle);
        if (state == null) {
            this.statesPerColumn.put(columnFamilyHandle, state = this.createColumnState(columnFamilyHandle));
        }
        return state;
    }

    protected ColumnState createColumnState(@NonNull ColumnFamilyHandle columnFamilyHandle) {
        return new ColumnState(columnFamilyHandle);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.statesPerColumn.values().stream().mapToLong(ColumnState::getDataSize).reduce(0L, Math::addExact);
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.statesPerColumn.values().stream().anyMatch((EPredicate<ColumnState>) ColumnState::isDirty);
    }

    @Override
    public void flush() throws Exception {
        this.statesPerColumn.values().parallelStream().forEach((EConsumer<ColumnState>) ColumnState::flush);

        //flush each of the column families which has pending data
        for (ColumnState columnState : this.statesPerColumn.values()) {
            synchronized (columnState) {
                if (!columnState.filesPendingIngestion.isEmpty()) {
                    this.database.delegate().ingestExternalFile(
                            columnState.column,
                            columnState.filesPendingIngestion.stream().map(Path::toString).collect(Collectors.toList()),
                            this.config.ingestOptions(DatabaseConfig.IngestType.MOVE));
                    columnState.filesPendingIngestion.forEach(this.database::returnTmpSstFilePath);
                    columnState.filesPendingIngestion.clear();
                }
            }
        }
    }

    @Override
    public void bulkFlush(@NonNull Collection<FlushableSstFileWriterBatch> toFlush) throws Exception {
        checkArg(toFlush.stream().allMatch(batch -> batch.database == this.database), "all instances must belong to this database!");

        //flush all column states in all batches
        toFlush.parallelStream().flatMap(batch -> batch.statesPerColumn.values().parallelStream()).forEach((EConsumer<ColumnState>) ColumnState::flush);

        //gather all the files into a single large batch
        Map<ColumnFamilyHandle, List<Path>> filesToIngest = new IdentityHashMap<>();
        for (FlushableSstFileWriterBatch batch : toFlush) {
            batch.statesPerColumn.forEach((columnFamilyHandle, columnState) -> {
                synchronized (columnState) {
                    if (!columnState.filesPendingIngestion.isEmpty()) {
                        filesToIngest.computeIfAbsent(columnFamilyHandle, unused -> new ArrayList<>()).addAll(columnState.filesPendingIngestion);
                        columnState.filesPendingIngestion.clear();
                    }
                }
            });
        }

        //ingest all the files
        filesToIngest.forEach((EBiConsumer<ColumnFamilyHandle, List<Path>>) (columnFamilyHandle, files) -> {
            this.database.delegate().ingestExternalFile(
                    columnFamilyHandle,
                    files.stream().map(Path::toString).collect(Collectors.toList()),
                    this.config.ingestOptions(DatabaseConfig.IngestType.MOVE));
            files.forEach(this.database::returnTmpSstFilePath);
            files.clear();
        });
    }

    @Override
    public void clear() throws Exception {
        this.statesPerColumn.values().forEach((EConsumer<ColumnState>) ColumnState::clear);
    }

    @Override
    public void close() throws Exception {
        this.flush();
        this.statesPerColumn.values().forEach((EConsumer<ColumnState>) ColumnState::close);
    }

    @Override
    public boolean threadSafe() {
        return false;
    }

    @Getter
    protected class ColumnState implements DBWriteAccess {
        protected static final long OPERATION_OVERHEAD = 8L; //we assume the overhead of a put/merge/delete is an additional 8 bytes

        protected final ColumnFamilyHandle column;
        protected final List<Path> filesPendingIngestion = new ArrayList<>();

        protected final Options options;

        protected Path currentFilePath;
        protected SstFileWriter writer;
        protected long dataSize;

        public ColumnState(@NonNull ColumnFamilyHandle column) {
            this.column = column;

            this.options = new Options(FlushableSstFileWriterBatch.this.config.dbOptions(), FlushableSstFileWriterBatch.this.database.columns().get(column).getOptions());
        }

        protected boolean isWriterOpen() {
            return this.currentFilePath != null;
        }

        @Override
        public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
            checkArg(columnFamilyHandle.equals(this.column), "mismatched column families!");
            this.getOpenWriter().put(key, value);
            this.dataSize += OPERATION_OVERHEAD + key.length + value.length;
        }

        @Override
        public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
            checkArg(columnFamilyHandle.equals(this.column), "mismatched column families!");
            int keyRemaining = key.remaining();
            int valueRemaining = value.remaining();
            this.getOpenWriter().put(key, value);
            this.dataSize += OPERATION_OVERHEAD + keyRemaining + valueRemaining;
        }

        @Override
        public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
            checkArg(columnFamilyHandle.equals(this.column), "mismatched column families!");
            this.getOpenWriter().merge(key, value);
            this.dataSize += OPERATION_OVERHEAD + key.length + value.length;
        }

        @Override
        public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
            checkArg(columnFamilyHandle.equals(this.column), "mismatched column families!");
            int keyRemaining = key.remaining();
            int valueRemaining = value.remaining();
            NativeRocksHelper.merge(this.getOpenWriter(), key, value);
            this.dataSize += OPERATION_OVERHEAD + keyRemaining + valueRemaining;
        }

        @Override
        public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
            checkArg(columnFamilyHandle.equals(this.column), "mismatched column families!");
            this.getOpenWriter().delete(key);
            this.dataSize += OPERATION_OVERHEAD + key.length;
        }

        @Override
        public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDataSize() {
            return this.dataSize;
        }

        protected SstFileWriter getOpenWriter() throws Exception {
            SstFileWriter writer = this.writer;
            if (writer == null) {
                writer = this.openWriter();
            }
            return writer;
        }

        protected SstFileWriter openWriter() throws Exception {
            checkState(this.writer == null, "already open!");
            checkState(this.currentFilePath == null, "path already exists!");

            this.dataSize = 0L;

            this.currentFilePath = FlushableSstFileWriterBatch.this.database.assignTmpSstFilePath(new String(this.column.getName(), StandardCharsets.UTF_8));
            try {
                checkState(!PFiles.checkFileExists(this.currentFilePath), "file '%s' already exists!", this.currentFilePath);
                PFiles.ensureFileExists(this.currentFilePath);

                try {
                    this.writer = new SstFileWriter(FlushableSstFileWriterBatch.this.config.envOptions(), this.options);
                    try {
                        this.writer.open(this.currentFilePath.toString());
                        return this.writer;
                    } catch (Exception e) {
                        try (SstFileWriter writer = this.writer) {
                            this.writer = null;
                        }
                        throw e;
                    }
                } catch (Exception e) {
                    if (PFiles.checkFileExists(this.currentFilePath)) {
                        PFiles.rm(this.currentFilePath);
                    }
                    throw e;
                }
            } catch (Exception e) {
                try {
                    FlushableSstFileWriterBatch.this.database.returnTmpSstFilePath(this.currentFilePath);
                } finally {
                    this.currentFilePath = null;
                }
                throw e;
            }
        }

        @Override
        public void flush() throws Exception {
            if (this.isWriterOpen()) {
                try (SstFileWriter writer = this.writer) {
                    this.writer = null;
                    this.dataSize = 0L;

                    //finish writing the SST file
                    writer.finish();

                    //queue the file path for ingestion
                    this.filesPendingIngestion.add(this.currentFilePath);
                    this.currentFilePath = null;
                } catch (Exception e) {
                    //delete the completed SST file without ingesting it
                    Files.delete(this.currentFilePath);
                    FlushableSstFileWriterBatch.this.database.returnTmpSstFilePath(this.currentFilePath);
                    this.currentFilePath = null;
                }
            }
        }

        @Override
        public void clear() throws Exception {
            if (this.isWriterOpen()) {
                try (SstFileWriter writer = this.writer) {
                    this.writer = null;
                    this.dataSize = 0L;
                } finally {
                    //delete the completed SST file without ingesting it
                    Files.delete(this.currentFilePath);
                    FlushableSstFileWriterBatch.this.database.returnTmpSstFilePath(this.currentFilePath);
                    this.currentFilePath = null;
                }
            }
        }

        @Override
        public void close() throws Exception {
            this.flush();

            this.options.close();
        }

        @Override
        public boolean threadSafe() {
            return false;
        }
    }

    /**
     * Implementation of {@link FlushableSstFileWriterBatch} with the ability to auto-flush SST files into the queue upon writing more than a given amount of data.
     *
     * @author DaPorkchop_
     */
    public static class AutoFlushing extends FlushableSstFileWriterBatch {
        protected final long threshold;

        public AutoFlushing(@NonNull DatabaseConfig config, @NonNull Database database, long threshold) {
            super(config, database);

            this.threshold = positive(threshold, "threshold");
        }

        @Override
        protected ColumnState createColumnState(@NonNull ColumnFamilyHandle columnFamilyHandle) {
            return new AutoFlushingColumnState(columnFamilyHandle);
        }

        protected class AutoFlushingColumnState extends ColumnState {
            public AutoFlushingColumnState(@NonNull ColumnFamilyHandle column) {
                super(column);
            }

            @Override
            public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
                super.put(columnFamilyHandle, key, value);
                this.checkFlush();
            }

            @Override
            public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
                super.put(columnFamilyHandle, key, value);
                this.checkFlush();
            }

            @Override
            public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
                super.merge(columnFamilyHandle, key, value);
                this.checkFlush();
            }

            @Override
            public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
                super.merge(columnFamilyHandle, key, value);
                this.checkFlush();
            }

            @Override
            public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
                super.delete(columnFamilyHandle, key);
                this.checkFlush();
            }

            @Override
            public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
                super.deleteRange(columnFamilyHandle, beginKey, endKey);
                this.checkFlush();
            }

            protected void checkFlush() throws Exception {
                if (this.dataSize() >= AutoFlushing.this.threshold) {
                    this.flush();
                }
            }
        }
    }
}
