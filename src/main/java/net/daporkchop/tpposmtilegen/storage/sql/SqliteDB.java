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

package net.daporkchop.tpposmtilegen.storage.sql;

import lombok.NonNull;
import net.daporkchop.lib.common.function.throwing.EBiConsumer;
import net.daporkchop.lib.common.function.throwing.EFunction;
import net.daporkchop.lib.primitive.list.LongList;
import net.daporkchop.lib.primitive.list.array.LongArrayList;
import net.daporkchop.lib.primitive.map.concurrent.ObjObjConcurrentHashMap;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.function.Function;

/**
 * @author DaPorkchop_
 */
public abstract class SqliteDB implements AutoCloseable {
    protected static final Map<Class<?>, long[]> PREPARED_STATEMENTS_OFFSETS_CACHE = new ObjObjConcurrentHashMap<>(); //faster computeIfAbsent
    protected static final Function<Class<?>, long[]> PREPARED_STATEMENTS_OFFSETS_COMPUTE_FUNC = (EFunction<Class<?>, long[]>) clazz -> {
        LongList list = new LongArrayList();
        do {
            for (Field field : clazz.getDeclaredFields()) {
                if (field.getType() == PreparedStatement.class || field.getType() == PreparedStatement[].class) {
                    list.add(PUnsafe.objectFieldOffset(field));
                }
            }
        } while ((clazz = clazz.getSuperclass()) != Object.class);
        return list.toArray();
    };

    protected static long[] getPreparedStatementOffsets(@NonNull Class<?> clazz) {
        return PREPARED_STATEMENTS_OFFSETS_CACHE.computeIfAbsent(clazz, PREPARED_STATEMENTS_OFFSETS_COMPUTE_FUNC);
    }

    public static SQLiteDataSource dataSource(@NonNull File file) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:file:" + file);
        return dataSource;
    }

    protected final Connection connection;

    public SqliteDB(@NonNull SQLiteDataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();
        this.connection.setAutoCommit(false);

        try (Statement statement = this.connection.createStatement()) {
            this.createTables().forEach((EBiConsumer<String, String>) (name, cols) ->
                    statement.addBatch("CREATE TABLE IF NOT EXISTS " + name + '(' + cols + ");"));

            statement.executeBatch();
        }
    }

    protected abstract Map<String, String> createTables();

    @Override
    public void close() throws SQLException {
        for (long off : getPreparedStatementOffsets(this.getClass())) {
            Object obj = PUnsafe.getObject(this, off);
            if (obj instanceof PreparedStatement) {
                ((PreparedStatement) obj).close();
            } else if (obj instanceof PreparedStatement[]) {
                for (PreparedStatement statement : (PreparedStatement[]) obj) {
                    statement.close();
                }
            }
        }

        this.connection.close();
    }
}
