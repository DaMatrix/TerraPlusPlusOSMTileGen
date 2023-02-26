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

import lombok.NonNull;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
final class SnapshotReadAccess implements DBReadAccess {
    protected final RocksDB db;

    protected final Snapshot snapshot;
    protected final Map<DatabaseConfig.ReadType, ReadOptions> readOptions = new EnumMap<>(DatabaseConfig.ReadType.class);

    public SnapshotReadAccess(@NonNull DatabaseConfig config, @NonNull RocksDB db) throws Exception {
        this.db = db;

        this.snapshot = db.getSnapshot();
        config.readOptionsByType().forEach((type, options) -> this.readOptions.put(type, new ReadOptions(options).setSnapshot(this.snapshot)));
    }

    @Override
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        return this.db.get(columnFamilyHandle, this.readOptions.get(DatabaseConfig.ReadType.GENERAL), key);
    }

    @Override
    public List<@NonNull byte[]> multiGetAsList(@NonNull List<@NonNull ColumnFamilyHandle> columnFamilyHandleList, @NonNull List<@NonNull byte[]> keys) throws Exception {
        return this.db.multiGetAsList(this.readOptions.get(DatabaseConfig.ReadType.GENERAL), columnFamilyHandleList, keys);
    }

    @Override
    public DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle) throws Exception {
        return new DBIterator.SimpleRocksIteratorWrapper(this.db.newIterator(columnFamilyHandle, this.readOptions.get(DatabaseConfig.ReadType.BULK_ITERATE)));
    }

    @Override
    public DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] fromInclusive, @NonNull byte[] toExclusive) throws Exception {
        return DBIterator.SimpleRangedRocksIteratorWrapper.from(this.db, columnFamilyHandle, this.readOptions.get(DatabaseConfig.ReadType.GENERAL), fromInclusive, toExclusive);
    }

    @Override
    public void close() throws Exception {
        this.readOptions.values().forEach(ReadOptions::close);
        this.snapshot.close();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }
}
