/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.storage.sql.point;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.storage.sql.SqliteDB;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import net.daporkchop.tpposmtilegen.util.persistent.PersistentMap;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class PointDB implements PersistentMap<Long, double[]> {
    static final Map<String, String> CREATE_TABLES = ImmutableMap.of(
            "points", ""
                      + "id INTEGER NOT NULL PRIMARY KEY,"
                      + " lon REAL NOT NULL,"
                      + " lat REAL NOT NULL");

    static final int FAST_BATCH_POINT_COUNT = 2000;

    protected final CloseableThreadLocal<ReadPointDB> read;
    protected final WritePointDB write;

    public PointDB(@NonNull Path path) throws SQLException {
        SQLiteDataSource dataSource = SqliteDB.dataSource(path.toFile());
        this.write = new WritePointDB(dataSource);
        this.read = CloseableThreadLocal.of(() -> new ReadPointDB(dataSource));
    }

    @Override
    public void put(@NonNull Long key, @NonNull double[] value) throws Exception {
        this.write.putPoint(key, value);
    }

    @Override
    public void putAll(@NonNull List<Long> keys, @NonNull List<double[]> values) throws Exception {
        checkArg(keys.size() == values.size(), "must have same number of keys as values!");

        if (!keys.isEmpty()) {
            this.write.putPoints(keys, values);
        }
    }

    @Override
    public double[] get(@NonNull Long key) throws Exception {
        return this.read.get().getPoint(key);
    }

    @Override
    public List<double[]> getAll(@NonNull List<Long> keys) throws Exception {
        int count = keys.size();
        long[] arr = new long[count];
        for (int i = 0; i < count; i++) {
            arr[i] = keys.get(i);
        }
        return Arrays.asList(this.read.get().getPoints(arr));
    }

    @Override
    public void flush() throws Exception {
        this.write.commit();
    }

    @Override
    public void close() throws Exception {
        this.read.close();
        this.write.close();
    }
}
