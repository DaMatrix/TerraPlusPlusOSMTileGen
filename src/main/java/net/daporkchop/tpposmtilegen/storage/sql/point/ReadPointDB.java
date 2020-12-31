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
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.storage.sql.SqliteDB;
import org.sqlite.SQLiteDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.storage.sql.point.PointDB.*;

/**
 * @author DaPorkchop_
 */
final class ReadPointDB extends SqliteDB {
    protected final PreparedStatement[] getPoints = new PreparedStatement[FAST_BATCH_POINT_COUNT];

    public ReadPointDB(@NonNull SQLiteDataSource dataSource) throws SQLException {
        super(dataSource);

        for (int i = 0; i < FAST_BATCH_POINT_COUNT; i++) {
            this.getPoints[i] = this.connection.prepareStatement(IntStream.rangeClosed(0, i)
                    .mapToObj(j -> "?")
                    .collect(Collectors.joining(", ", "SELECT lon, lat FROM points WHERE id IN (", ");")));
        }
    }

    @Override
    protected Map<String, String> createTables() {
        return CREATE_TABLES;
    }

    public double[] getPoint(long id) throws SQLException {
        PreparedStatement statement = this.getPoints[0];
        statement.setLong(1, id);

        try (ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? new double[]{ resultSet.getDouble(1), resultSet.getDouble(2) } : null;
        } finally {
            statement.clearParameters();
        }
    }

    public double[][] getPoints(@NonNull long... ids) throws SQLException {
        positive(ids.length, "ids.length");
        checkArg(ids.length <= FAST_BATCH_POINT_COUNT, "can get at most %d points at a time!", FAST_BATCH_POINT_COUNT);

        PreparedStatement statement = this.getPoints[ids.length - 1];
        for (int i = 0; i < ids.length; i++) {
            statement.setLong(i + 1, ids[i]);
        }

        try (ResultSet resultSet = statement.executeQuery()) {
            double[][] points = new double[ids.length][];
            int i = 0;
            while (resultSet.next()) {
                points[i] = new double[]{ resultSet.getDouble(1), resultSet.getDouble(2) };
                i++;
            }
            checkState(i == points.length, "not all points were found!");
            return points;
        } finally {
            statement.clearParameters();
        }
    }
}
