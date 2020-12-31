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

import lombok.NonNull;
import net.daporkchop.tpposmtilegen.storage.sql.WriteableSqliteDB;
import org.sqlite.SQLiteDataSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.storage.sql.point.PointDB.*;

/**
 * @author DaPorkchop_
 */
final class WritePointDB extends WriteableSqliteDB {
    protected final PreparedStatement createPoint;
    protected final PreparedStatement modifyPoint;
    protected final PreparedStatement[] deletePoint = new PreparedStatement[FAST_BATCH_POINT_COUNT];

    public WritePointDB(@NonNull SQLiteDataSource dataSource) throws SQLException {
        super(dataSource);

        this.createPoint = this.connection.prepareStatement("INSERT INTO points VALUES (?, ?, ?);");
        this.modifyPoint = this.connection.prepareStatement("UPDATE points SET lon = ?, lat = ? WHERE id = ?;");

        for (int i = 0; i < FAST_BATCH_POINT_COUNT; i++) {
            this.deletePoint[i] = this.connection.prepareStatement(IntStream.rangeClosed(0, i)
                    .mapToObj(j -> "?")
                    .collect(Collectors.joining(", ", "DELETE FROM points WHERE id IN (", ");")));
        }
    }

    @Override
    protected Map<String, String> createTables() {
        return CREATE_TABLES;
    }

    synchronized void putPoint(long id, double[] point) throws SQLException {
        this.createPoint.setLong(1, id);
        this.createPoint.setDouble(2, point[0]);
        this.createPoint.setDouble(3, point[1]);
        this.createPoint.execute();
        this.createPoint.clearParameters();
        this.uncommitedChanges = true;
    }

    synchronized void putPoints(@NonNull List<Long> keys, @NonNull List<double[]> values) throws SQLException {
        for (int i = 0, count = keys.size(); i < count; i++) {
            this.createPoint.setLong(1, keys.get(i));
            double[] point = values.get(i);
            this.createPoint.setDouble(2, point[0]);
            this.createPoint.setDouble(3, point[1]);
            this.createPoint.addBatch();
        }
        this.createPoint.clearParameters();
        this.createPoint.executeBatch();
        this.uncommitedChanges = true;
    }

    synchronized void modifyPoints(@NonNull List<Long> keys, @NonNull List<double[]> values) throws SQLException {
        for (int i = 0, count = keys.size(); i < count; i++) {
            this.modifyPoint.setLong(3, keys.get(i));
            double[] point = values.get(i);
            this.modifyPoint.setDouble(1, point[0]);
            this.modifyPoint.setDouble(2, point[1]);
            this.modifyPoint.addBatch();
        }
        this.modifyPoint.clearParameters();
        this.modifyPoint.executeBatch();
        this.uncommitedChanges = true;
    }

    synchronized void deletePoints(@NonNull long... ids) throws SQLException {
        positive(ids.length, "ids.length");
        checkArg(ids.length <= FAST_BATCH_POINT_COUNT, "can delete at most %d nodes at a time!", FAST_BATCH_POINT_COUNT);

        PreparedStatement statement = this.deletePoint[ids.length - 1];
        for (int i = 0; i < ids.length; i++) {
            statement.setLong(i + 1, ids[i]);
        }
        statement.execute();
        statement.clearParameters();
        this.uncommitedChanges = true;
    }
}
