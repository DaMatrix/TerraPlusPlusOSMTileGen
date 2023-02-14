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
import lombok.Setter;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public final class Database implements AutoCloseable {
    @Getter
    private final RocksDB delegate;
    @Getter
    private final List<ColumnFamilyHandle> columns;

    @Getter
    private final DatabaseConfig config;

    private final DBWriteAccess batch;
    private final DBAccess readWriteBatch;

    @Getter
    private final DBReadAccess read;

    private Database(@NonNull RocksDB delegate, @NonNull List<ColumnFamilyHandle> columns, @NonNull DatabaseConfig config, boolean autoFlush) {
        this.delegate = delegate;
        this.config = config;
        this.columns = new CopyOnWriteArrayList<>(columns);
        this.batch = new ThreadLocalDBWriteAccess(new CloseableThreadLocal<DBWriteAccess>() {
            @Override
            protected DBWriteAccess initialValue0() throws Exception {
                return autoFlush ? new AutoFlushingWriteBatch(config, delegate, 64L << 20L) : new FlushableWriteBatch(config, delegate);
            }
        });
        this.read = new DirectReadAccess(config, delegate);
        this.readWriteBatch = new ReadWriteBatchAccess(this.read, this.batch);
    }

    public DBWriteAccess batch() {
        checkState(this.delegate instanceof OptimisticTransactionDB, "storage is open in read-only mode!");
        return this.batch;
    }

    public DBAccess readWriteBatch() {
        checkState(this.delegate instanceof OptimisticTransactionDB, "storage is open in read-only mode!");
        return this.readWriteBatch;
    }

    public DBWriteAccess newNotAutoFlushingWriteBatch() {
        checkState(this.delegate instanceof OptimisticTransactionDB, "storage is open in read-only mode!");
        return new FlushableWriteBatch(this.config, this.delegate);
    }

    public DBAccess newTransaction() {
        checkState(this.delegate instanceof OptimisticTransactionDB, "storage is open in read-only mode!");
        OptimisticTransactionDB delegate = (OptimisticTransactionDB) this.delegate;
        return new TransactionAccess(this.config, delegate, delegate.beginTransaction(this.config.writeOptions(DatabaseConfig.WriteType.SYNC)));
    }

    public void flush() throws Exception {
        this.batch.flush();
    }

    public void clear(@NonNull List<? extends WrappedRocksDB> toClear) throws Exception {
        checkState(this.delegate instanceof OptimisticTransactionDB, "storage is open in read-only mode!");
        checkArg(toClear.stream().allMatch(wrapper -> wrapper.database == this), "all wrappers must belong to this database instance!");

        List<ColumnFamilyHandle> oldColumns = toClear.stream().map(wrapper -> wrapper.column).collect(Collectors.toList());
        //noinspection SlowListContainsAll
        checkState(this.columns.containsAll(oldColumns), "a wrapper contains a column family which doesn't exist?");
        this.columns.removeAll(oldColumns);

        this.flush();
        this.delegate.dropColumnFamilies(oldColumns);
        oldColumns.forEach(ColumnFamilyHandle::close);

        List<ColumnFamilyHandle> newColumns = this.delegate.createColumnFamilies(toClear.stream().map(wrapper -> wrapper.desc).collect(Collectors.toList()));
        this.columns.addAll(newColumns);
        for (int i = 0; i < toClear.size(); i++) {
            toClear.get(i).column = newColumns.get(i);
        }
    }

    public ColumnFamilyHandle nukeAndReplaceColumnFamily(@NonNull ColumnFamilyHandle old, @NonNull ColumnFamilyDescriptor desc) throws Exception {
        checkState(this.columns.remove(old), "column doesn't exist?!?");
        this.flush();
        this.delegate.dropColumnFamily(old);
        old.close();
        ColumnFamilyHandle column = this.delegate.createColumnFamily(desc);
        this.columns.add(column);
        return column;
    }

    @Override
    public void close() throws Exception {
        this.batch.close();

        if (!this.config.readOnly()) {
            this.delegate.flush(this.config.flushOptions(DatabaseConfig.FlushType.GENERAL), this.columns);
        }

        this.columns.forEach(ColumnFamilyHandle::close);
        this.delegate.close();
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

        public Builder add(@NonNull String name, @NonNull DatabaseConfig.ColumnFamilyType type, @NonNull Factory factory) {
            this.columns.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), this.config.columnFamilyOptions(type)));
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
                        : OptimisticTransactionDB.open(this.config.dbOptions(), path.toString(), this.columns, columns);
            } catch (RocksDBException e) {
                String s = e.getMessage().replace("Column families not opened: ", "");
                if (!this.config.readOnly() && s.length() != e.getMessage().length()) {
                    for (String name : s.split(", ")) {
                        logger.warn("Deleting column family: %s", name);
                        this.columns.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), this.config.columnFamilyOptions(DatabaseConfig.ColumnFamilyType.FAST)));
                    }
                    db = OptimisticTransactionDB.open(this.config.dbOptions(), path.toString(), this.columns, columns);
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
            Database database = new Database(db, columns, this.config, this.autoFlush);

            for (int i = 1; i < this.factories.size(); i++) {
                this.factories.get(i).accept(database, columns.get(i), this.columns.get(i));
            }

            return database;
        }
    }
}
