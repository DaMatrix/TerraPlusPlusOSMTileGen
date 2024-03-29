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

import io.netty.util.concurrent.FastThreadLocalThread;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.MergeOperator;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public final class Database implements AutoCloseable {
    @Getter
    private final RocksDB delegate;
    @Getter
    private final Map<ColumnFamilyHandle, ColumnFamilyDescriptor> columns;

    @Getter
    private final DatabaseConfig config;

    private final DBWriteAccess batch;
    private final DBAccess readWriteBatch;
    private final DBWriteAccess sstBatch;

    @Getter
    private final DBReadAccess read;

    @Getter
    private final Path path;

    private final Path tmpSstDirectoryPath;
    private final Set<Path> activeTmpSstFiles = new TreeSet<>();

    private Database(@NonNull RocksDB delegate, @NonNull Map<ColumnFamilyHandle, ColumnFamilyDescriptor> columns, @NonNull DatabaseConfig config, boolean autoFlush, @NonNull Path path) {
        this.delegate = delegate;
        this.config = config;
        this.path = path;

        this.columns = Collections.synchronizedMap(new Object2ObjectOpenHashMap<>(columns));

        this.tmpSstDirectoryPath = path.resolve("tmp_sst_files");
        PFiles.rm(this.tmpSstDirectoryPath);

        this.batch = new ThreadLocalDBWriteAccess(new CloseableThreadLocal<DBWriteAccess>() {
            @Override
            protected DBWriteAccess initialValue0() throws Exception {
                DatabaseConfig.WriteType writeType = DatabaseConfig.WriteType.NO_WAL;
                return autoFlush
                        ? new FlushableWriteBatch.AutoFlushing(config, writeType, delegate, 64L << 20L)
                        : new FlushableWriteBatch.Regular(config, writeType, delegate);
            }
        });
        this.sstBatch = new ThreadLocalDBWriteAccess(new CloseableThreadLocal<DBWriteAccess>() {
            @Override
            protected DBWriteAccess initialValue0() throws Exception {
                return autoFlush
                        ? new FlushableSstFileWriterBatch.AutoFlushing(config, Database.this, 128L << 20L)
                        : new FlushableSstFileWriterBatch(config, Database.this);
            }
        });

        this.read = new DirectReadAccess(config, delegate);
        this.readWriteBatch = new ReadWriteBatchAccess(this.read, this.batch);
    }

    public DBWriteAccess batch() {
        checkState(!(Thread.currentThread() instanceof ForkJoinWorkerThread), "not allowed to be accessed from a ForkJoinWorkerThread!");
        checkState(!this.config.readOnly(), "storage is open in read-only mode!");
        return this.batch;
    }

    /**
     * Creates a new batched write operation which is managed by the caller.
     * <p>
     * The returned {@link DBWriteAccess} <strong>must</strong> be explicitly closed by the caller.
     *
     * @return a new batched write operation
     */
    public DBWriteAccess beginLocalBatch() {
        return this.beginLocalBatch(DatabaseConfig.WriteType.GENERAL);
    }

    /**
     * Creates a new batched write operation which is managed by the caller.
     * <p>
     * The returned {@link DBWriteAccess} <strong>must</strong> be explicitly closed by the caller.
     *
     * @return a new batched write operation
     */
    public DBWriteAccess beginLocalBatch(@NonNull DatabaseConfig.WriteType writeType) {
        checkState(!this.config.readOnly(), "storage is open in read-only mode!");
        return new FlushableWriteBatch.Regular(this.config, writeType, this.delegate);
    }

    public DBAccess readWriteBatch() {
        checkState(!(Thread.currentThread() instanceof ForkJoinWorkerThread), "not allowed to be accessed from a ForkJoinWorkerThread!");
        checkState(!this.config.readOnly(), "storage is open in read-only mode!");
        return this.readWriteBatch;
    }

    public DBWriteAccess sstBatch() {
        checkState(!(Thread.currentThread() instanceof ForkJoinWorkerThread), "not allowed to be accessed from a ForkJoinWorkerThread!");
        checkState(!this.config.readOnly(), "storage is open in read-only mode!");
        return this.sstBatch;
    }

    public DBAccess newTransaction() {
        return this.newTransaction(DatabaseConfig.WriteType.GENERAL);
    }

    public DBAccess newTransaction(@NonNull DatabaseConfig.WriteType writeType) {
        checkState(!this.config.readOnly(), "storage is open in read-only mode!");
        OptimisticTransactionDB delegate = (OptimisticTransactionDB) this.delegate;
        return new TransactionAccess(this.config, delegate, delegate.beginTransaction(this.config.writeOptions(writeType)));
    }

    public DBReadAccess snapshot() throws Exception {
        return new SnapshotReadAccess(this.config, this.delegate);
    }

    public void flush() throws Exception {
        this.batch.flush();
    }

    public ColumnFamilyHandle internalColumnFamily(@NonNull WrappedRocksDB wrapper) {
        checkArg(wrapper.database == this, "given wrapper must belong to this database instance!");
        return wrapper.column;
    }

    public void clear(@NonNull List<? extends WrappedRocksDB> toClear) throws Exception {
        checkState(!this.config.readOnly(), "storage is open in read-only mode!");
        checkArg(toClear.stream().allMatch(wrapper -> wrapper.database == this), "all wrappers must belong to this database instance!");

        toClear = toClear.stream().distinct()
                .filter(wrapper -> !wrapper.isGuaranteedEmpty()) //skip processing any wrappers which are already empty
                .collect(Collectors.toList());

        if (toClear.isEmpty()) { //all the wrappers were empty, we don't have to do anything
            return;
        }

        List<ColumnFamilyHandle> oldColumns = toClear.stream().map(wrapper -> wrapper.column).distinct().collect(Collectors.toList());
        checkState(oldColumns.size() == toClear.size(), "number of distinct columns (%d) != number of distinct wrappers to clear (%d)",
                oldColumns.size(), toClear.size());
        checkState(oldColumns.stream().allMatch(this.columns::containsKey), "a wrapper contains a column family which doesn't exist?");
        oldColumns.forEach(this.columns::remove);

        this.flush();
        this.delegate.dropColumnFamilies(oldColumns);
        oldColumns.forEach(ColumnFamilyHandle::close);

        List<ColumnFamilyHandle> newColumns = this.delegate.createColumnFamilies(toClear.stream().map(wrapper -> wrapper.desc).collect(Collectors.toList()));
        checkState(newColumns.stream().noneMatch(this.columns::containsKey), "one of the new column families is already present?!?");
        checkState(new HashSet<>(newColumns).size() == newColumns.size(), "the new column families aren't distinct?!?");
        for (int i = 0; i < toClear.size(); i++) {
            WrappedRocksDB wrapper = toClear.get(i);
            ColumnFamilyHandle newColumn = newColumns.get(i);

            wrapper.column = newColumn;
            this.columns.put(newColumn, wrapper.desc);
        }
    }

    public ColumnFamilyHandle nukeAndReplaceColumnFamily(@NonNull ColumnFamilyHandle old, @NonNull ColumnFamilyDescriptor desc) throws Exception {
        if (this.delegate.getColumnFamilyMetaData(old).size() == 0L) { //skip and do nothing
            return old;
        }

        checkState(this.columns.remove(old) != null, "column doesn't exist?!?");
        this.flush();
        this.delegate.dropColumnFamily(old);
        old.close();
        ColumnFamilyHandle column = this.delegate.createColumnFamily(desc);
        checkState(this.columns.putIfAbsent(column, desc) == null, "the new column family is already present?!?");
        return column;
    }

    synchronized Path assignTmpSstFilePath(@NonNull String prefix) {
        Path path;
        do {
            path = this.tmpSstDirectoryPath.resolve(prefix + '-' + UUID.randomUUID() + ".sst");
        } while (!this.activeTmpSstFiles.add(path));
        return path;
    }

    synchronized void returnTmpSstFilePath(@NonNull Path path) {
        checkArg(path.startsWith(this.tmpSstDirectoryPath), "given path '%s' should start with '%s'", path, this.tmpSstDirectoryPath);
        checkArg(this.activeTmpSstFiles.remove(path), "given path '%s' isn't assigned or has already been returned", path);
        checkArg(!PFiles.checkFileExists(path), "given path '%s' exists", path);
    }

    @Override
    public synchronized void close() throws Exception {
        this.flush();
        this.batch.close();

        if (!this.config.readOnly()) { //flush all WALs
            this.delegate.flush(this.config.flushOptions(DatabaseConfig.FlushType.GENERAL), new ArrayList<>(this.columns.keySet()));
        }

        this.columns.keySet().forEach(ColumnFamilyHandle::close);
        this.delegate.close();

        if (!this.activeTmpSstFiles.isEmpty()) {
            logger.alert("some temporary SST files have not been ingested:\n\n" + this.activeTmpSstFiles);
        }
        PFiles.rm(this.tmpSstDirectoryPath);
    }

    @FunctionalInterface
    public interface Factory {
        void accept(@NonNull Database database, @NonNull ColumnFamilyHandle handle, @NonNull ColumnFamilyDescriptor descriptor);
    }

    @Setter
    public static final class Builder {
        private final DatabaseConfig config;
        private final List<ColumnFamilyDescriptor> columns = new ArrayList<>();
        private final List<Factory> factories = new ArrayList<>();
        private boolean autoFlush;

        public Builder(@NonNull DatabaseConfig config) {
            this.config = config;
            this.columns.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, this.config.columnFamilyOptions(DatabaseConfig.ColumnFamilyType.FAST)));
            this.factories.add(null);
        }

        public Builder add(@NonNull String name, @NonNull Factory factory) {
            return this.add(name, DatabaseConfig.ColumnFamilyType.FAST, factory);
        }

        public Builder add(@NonNull String name, @NonNull Factory factory, MergeOperator mergeOperator) {
            return this.add(name, DatabaseConfig.ColumnFamilyType.FAST, factory, mergeOperator);
        }

        public Builder add(@NonNull String name, @NonNull DatabaseConfig.ColumnFamilyType type, @NonNull Factory factory) {
            return this.add(name, type, factory, null);
        }

        public Builder add(@NonNull String name, @NonNull DatabaseConfig.ColumnFamilyType type, @NonNull Factory factory, MergeOperator mergeOperator) {
            if (mergeOperator != null) {
                this.columns.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions(this.config.columnFamilyOptions(type))
                        .setMergeOperator(mergeOperator)));
            } else {
                this.columns.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), this.config.columnFamilyOptions(type)));
            }
            this.factories.add(factory);
            return this;
        }

        public Database build(@NonNull Path path) throws Exception {
            List<ColumnFamilyHandle> columns = new ArrayList<>(this.columns.size());

            RocksDB db;
            OPEN_DB:
            try {
                db = this.config.readOnly()
                        ? RocksDB.openReadOnly(this.config.dbOptions(), path.toString(), this.columns, columns)
                        : this.config.transactional()
                        ? OptimisticTransactionDB.open(this.config.dbOptions(), path.toString(), this.columns, columns)
                        : RocksDB.open(this.config.dbOptions(), path.toString(), this.columns, columns);
            } catch (RocksDBException e) {
                String s = e.getMessage().replace("Column families not opened: ", "");
                if (!this.config.readOnly() && s.length() != e.getMessage().length()) {
                    for (String name : s.split(", ")) {
                        logger.warn("Deleting column family: %s", name);
                        this.columns.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), this.config.columnFamilyOptions(DatabaseConfig.ColumnFamilyType.FAST)));
                    }
                    db = this.config.transactional()
                            ? OptimisticTransactionDB.open(this.config.dbOptions(), path.toString(), this.columns, columns)
                            : RocksDB.open(this.config.dbOptions(), path.toString(), this.columns, columns);
                    while (columns.size() > this.factories.size()) {
                        ColumnFamilyHandle handle = columns.remove(this.factories.size());
                        this.columns.remove(this.factories.size());
                        db.dropColumnFamily(handle);
                        handle.close();
                    }
                    break OPEN_DB;
                }
                throw e;
            }

            checkState(columns.size() == this.columns.size());
            Map<ColumnFamilyHandle, ColumnFamilyDescriptor> columnsToDescriptorsMap = IntStream.range(0, columns.size()).boxed()
                    .collect(Collectors.toMap(columns::get, this.columns::get));

            Database database = new Database(db, columnsToDescriptorsMap, this.config, this.autoFlush, path);

            for (int i = 1; i < this.factories.size(); i++) {
                this.factories.get(i).accept(database, columns.get(i), this.columns.get(i));
            }

            return database;
        }
    }
}
