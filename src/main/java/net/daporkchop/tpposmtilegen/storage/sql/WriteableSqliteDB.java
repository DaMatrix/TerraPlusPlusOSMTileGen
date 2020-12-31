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
import net.daporkchop.lib.unsafe.PUnsafe;
import org.sqlite.SQLiteDataSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public abstract class WriteableSqliteDB extends SqliteDB {
    protected int writeQueue = 0;
    protected boolean uncommitedChanges = false;

    public WriteableSqliteDB(@NonNull SQLiteDataSource dataSource) throws SQLException {
        super(dataSource);
    }

    protected void incrementWriteQueue() throws SQLException {
        if (++this.writeQueue == 100000) {
            checkState(this.flushWriteQueue(), "write queue was applied, but caused no changes?!?");
        }
    }

    protected boolean flushWriteQueue() throws SQLException {
        this.writeQueue = 0;
        int maxAffected = 0;
        for (long off : getPreparedStatementOffsets(this.getClass())) {
            Object obj = PUnsafe.getObject(this, off);
            if (obj instanceof PreparedStatement) {
                for (int affected : ((PreparedStatement) obj).executeBatch()) {
                    maxAffected = max(maxAffected, affected);
                }
            } else if (obj instanceof PreparedStatement[]) {
                for (PreparedStatement statement : (PreparedStatement[]) obj) {
                    for (int affected : statement.executeBatch()) {
                        maxAffected = max(maxAffected, affected);
                    }
                }
            }
        }

        if (maxAffected != 0) {
            this.uncommitedChanges = true;
        }
        return maxAffected != 0;
    }

    public synchronized void commit() throws SQLException {
        if (this.uncommitedChanges | (this.writeQueue != 0 && this.flushWriteQueue())) {
            this.uncommitedChanges = false;
            System.out.println(Thread.currentThread() + " committing");
            this.connection.commit();
            System.out.println(Thread.currentThread() + " committed.");
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        this.commit();

        super.close();
    }
}
