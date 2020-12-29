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

package net.daporkchop.tpposmtilegen.storage;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.persistence.disk.DiskPersistence;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.StringJoiner;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.storage.OSMAttributes.*;

/**
 * @author DaPorkchop_
 */
@Getter
public class Storage implements AutoCloseable {
    protected static final Map<Class<?>, String> SQL_TYPE_NAMES = new IdentityHashMap<>();

    static {
        SQL_TYPE_NAMES.put(Byte.class, "INTEGER");
        SQL_TYPE_NAMES.put(Short.class, "INTEGER");
        SQL_TYPE_NAMES.put(Character.class, "INTEGER");
        SQL_TYPE_NAMES.put(Integer.class, "INTEGER");
        SQL_TYPE_NAMES.put(Long.class, "INTEGER");
        SQL_TYPE_NAMES.put(Float.class, "REAL");
        SQL_TYPE_NAMES.put(Double.class, "REAL");
        SQL_TYPE_NAMES.put(byte[].class, "BLOB");
        SQL_TYPE_NAMES.put(String.class, "TEXT");
    }

    @Deprecated
    public static IndexedCollection<Element> openStorage(@NonNull File root) {
        IndexedCollection<Element> storage = new ConcurrentIndexedCollection<>(DiskPersistence.onPrimaryKeyInFile(ID, new File(root, "index.dat")));
        //storage.addIndex(DiskIndex.onAttribute(MIN_X));
        //storage.addIndex(DiskIndex.onAttribute(MAX_X));
        //storage.addIndex(DiskIndex.onAttribute(MIN_Z));
        //storage.addIndex(DiskIndex.onAttribute(MAX_Z));
        //storage.addIndex(DiskIndex.onAttribute(CHILDREN));
        //storage.addIndex(DiskIndex.onAttribute(TAGS));
        return storage;
    }

    public static FastThreadLocal<Storage> threadLocalStorage(@NonNull File root) {
        SQLiteDataSource dataSource = dataSource(root);

        return new CloseableThreadLocal<Storage>() {
            @Override
            protected Storage initialValue0() throws Exception {
                return new Storage(dataSource);
            }
        };
    }

    public static SQLiteDataSource dataSource(@NonNull File file) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:file:" + new File(PFiles.ensureDirectoryExists(file), "index.dat"));
        return dataSource;
    }

    @Getter(AccessLevel.NONE)
    protected final Connection connection;

    protected final PreparedStatement putNode;
    protected final PreparedStatement getAllNodes;
    protected final PreparedStatement getNodeById;
    protected final PreparedStatement countNodes;

    public Storage(@NonNull SQLiteDataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();

        try (Statement statement = this.connection.createStatement()) {
            statement.addBatch("CREATE TABLE IF NOT EXISTS nodes("
                               + "id INTEGER NOT NULL PRIMARY KEY,"
                               + "lon REAL NOT NULL,"
                               + "lat REAL NOT NULL,"
                               + "tags BLOB"
                               + ");");

            statement.executeBatch();
        }

        this.connection.setAutoCommit(false);

        this.putNode = this.connection.prepareStatement("INSERT OR REPLACE INTO nodes VALUES (?, ?, ?, ?);");
        this.getAllNodes = this.connection.prepareStatement("SELECT * FROM nodes;");
        this.getNodeById = this.connection.prepareStatement("SELECT * FROM nodes WHERE id = ?;");
        this.countNodes = this.connection.prepareStatement("SELECT count(*) FROM nodes;");
    }

    public String nodeToString(@NonNull ResultSet resultSet) throws SQLException {
        StringJoiner joiner = new StringJoiner(", ");
        joiner.add("id=" + resultSet.getLong(1));
        joiner.add("lon=" + resultSet.getDouble(2));
        joiner.add("lat=" + resultSet.getDouble(3));
        joiner.add("tags=" + resultSet.getBytes(4));
        return joiner.toString();
    }

    public Array createArray(@NonNull Object... elements) throws SQLException {
        String typeName = SQL_TYPE_NAMES.get(elements.getClass().getComponentType());
        checkArg(typeName != null, "unsupported array: %s", elements);
        return this.connection.createArrayOf(typeName, elements);
    }

    public void commit() throws SQLException {
        this.connection.commit();
    }

    @Override
    public void close() throws SQLException {
        try { //close all prepared statements
            for (Field field : Storage.class.getDeclaredFields()) {
                if (field.getType() == PreparedStatement.class) {
                    PreparedStatement statement = (PreparedStatement) field.get(this);
                    statement.close();
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        this.connection.close();
    }
}
