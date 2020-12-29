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

import io.netty.util.concurrent.FastThreadLocal;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class NodeDB implements AutoCloseable {
    protected static final int FAST_BATCH_NODE_COUNT = 2000;

    public static FastThreadLocal<NodeDB> threadLocalStorage(@NonNull File root) {
        SQLiteDataSource dataSource = dataSource(root);

        return new CloseableThreadLocal<NodeDB>() {
            @Override
            protected NodeDB initialValue0() throws Exception {
                return new NodeDB(dataSource);
            }
        };
    }

    public static SQLiteDataSource dataSource(@NonNull File file) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:file:" + file);
        return dataSource;
    }

    protected final Connection connection;

    protected final PreparedStatement createNode;
    protected final PreparedStatement modifyNode;
    protected final PreparedStatement[] deleteNodes = new PreparedStatement[FAST_BATCH_NODE_COUNT];
    protected final PreparedStatement[] getNodes = new PreparedStatement[FAST_BATCH_NODE_COUNT];

    protected int writeQueue = 0;

    public NodeDB(@NonNull SQLiteDataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();

        try (Statement statement = this.connection.createStatement()) {
            statement.addBatch("CREATE TABLE IF NOT EXISTS nodes("
                               + "id INTEGER NOT NULL PRIMARY KEY,"
                               + "data BLOB NOT NULL"
                               + ");");

            statement.executeBatch();
        }

        this.connection.setAutoCommit(false);

        this.createNode = this.connection.prepareStatement("INSERT INTO nodes VALUES (?, ?);");
        this.modifyNode = this.connection.prepareStatement("UPDATE nodes SET data = ? WHERE id = ?;");

        for (int i = 0; i < FAST_BATCH_NODE_COUNT; i++) {
            this.deleteNodes[i] = this.connection.prepareStatement(IntStream.rangeClosed(0, i)
                    .mapToObj(j -> "?")
                    .collect(Collectors.joining(", ", "DELETE FROM nodes WHERE id IN (", ");")));
            this.getNodes[i] = this.connection.prepareStatement(IntStream.rangeClosed(0, i)
                    .mapToObj(j -> "?")
                    .collect(Collectors.joining(", ", "SELECT data FROM nodes WHERE id IN (", ");")));
        }
    }

    public void createNode(@NonNull Node node) throws SQLException {
        this.createNode.setLong(1, node.id());
        this.createNode.setBytes(2, node.toByteArray());
        this.createNode.addBatch();
        this.createNode.clearParameters();

        this.incrementWriteQueue();
    }

    public void modifyNode(@NonNull Node node) throws SQLException {
        this.modifyNode.setLong(2, node.id());
        this.modifyNode.setBytes(1, node.toByteArray());
        this.modifyNode.addBatch();
        this.modifyNode.clearParameters();

        this.incrementWriteQueue();
    }

    public void deleteNodes(@NonNull long... ids) throws SQLException {
        positive(ids.length, "ids.length");
        checkArg(ids.length <= FAST_BATCH_NODE_COUNT, "can delete at most %d nodes at a time!", FAST_BATCH_NODE_COUNT);

        PreparedStatement statement = this.deleteNodes[ids.length - 1];
        for (int i = 0; i < ids.length; i++) {
            statement.setLong(i + 1, ids[i]);
        }
        statement.addBatch();
        statement.clearParameters();

        this.incrementWriteQueue();
    }

    public Node[] getNodes(@NonNull long... ids) throws SQLException {
        positive(ids.length, "ids.length");
        checkArg(ids.length <= FAST_BATCH_NODE_COUNT, "can get at most %d nodes at a time!", FAST_BATCH_NODE_COUNT);

        PreparedStatement statement = this.getNodes[ids.length - 1];
        for (int i = 0; i < ids.length; i++) {
            statement.setLong(i + 1, ids[i]);
        }

        try (ResultSet resultSet = statement.executeQuery()) {
            Node[] nodes = new Node[ids.length];
            int i = 0;
            while (resultSet.next()) {
                nodes[i] = new Node(ids[i], resultSet.getBytes(1));
                i++;
            }
            checkState(i == nodes.length, "not all nodes were found!");
            return nodes;
        } finally {
            statement.clearParameters();
        }
    }

    protected void incrementWriteQueue() throws SQLException {
        if (++this.writeQueue == 100000) {
            this.flushWriteQueue();
        }
    }

    protected void flushWriteQueue() throws SQLException {
        this.writeQueue = 0;
        this.modifyNode.executeBatch();
        this.createNode.executeBatch();
        for (PreparedStatement statement : this.deleteNodes) {
            statement.executeBatch();
        }
    }

    public void commit() throws SQLException {
        if (this.writeQueue != 0) {
            this.flushWriteQueue();
        }

        this.connection.commit();
    }

    @Override
    public void close() throws SQLException {
        try {
            this.commit();

            try { //close all prepared statements
                for (Field field : NodeDB.class.getDeclaredFields()) {
                    if (field.getType() == PreparedStatement.class) {
                        ((PreparedStatement) field.get(this)).close();
                    } else if (field.getType() == PreparedStatement[].class) {
                        for (PreparedStatement statement : (PreparedStatement[]) field.get(this)) {
                            statement.close();
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            this.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
