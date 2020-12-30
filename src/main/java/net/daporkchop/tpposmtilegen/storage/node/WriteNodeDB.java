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

package net.daporkchop.tpposmtilegen.storage.node;

import lombok.NonNull;
import net.daporkchop.tpposmtilegen.storage.Node;
import net.daporkchop.tpposmtilegen.storage.sql.WriteableSqliteDB;
import org.sqlite.SQLiteDataSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.storage.node.NodeDB.*;

/**
 * @author DaPorkchop_
 */
public class WriteNodeDB extends WriteableSqliteDB {
    protected final PreparedStatement createNode;
    protected final PreparedStatement modifyNode;
    protected final PreparedStatement[] deleteNodes = new PreparedStatement[FAST_BATCH_NODE_COUNT];

    public WriteNodeDB(@NonNull SQLiteDataSource dataSource) throws SQLException {
        super(dataSource);

        this.createNode = this.connection.prepareStatement("INSERT INTO nodes VALUES (?, ?);");
        this.modifyNode = this.connection.prepareStatement("UPDATE nodes SET data = ? WHERE id = ?;");

        for (int i = 0; i < FAST_BATCH_NODE_COUNT; i++) {
            this.deleteNodes[i] = this.connection.prepareStatement(IntStream.rangeClosed(0, i)
                    .mapToObj(j -> "?")
                    .collect(Collectors.joining(", ", "DELETE FROM nodes WHERE id IN (", ");")));
        }
    }

    @Override
    protected Map<String, String> createTables() {
        return CREATE_TABLES;
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
}
