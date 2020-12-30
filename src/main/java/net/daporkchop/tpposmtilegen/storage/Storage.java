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

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.storage.sql.SqliteDB;
import net.daporkchop.tpposmtilegen.storage.sql.node.ReadNodeDB;
import net.daporkchop.tpposmtilegen.storage.sql.node.WriteNodeDB;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.rocksdb.RocksDB;
import org.sqlite.SQLiteDataSource;

import java.io.File;

/**
 * @author DaPorkchop_
 */
public class Storage implements AutoCloseable {
    @Getter
    protected final WriteNodeDB writeNodeDB;
    protected final CloseableThreadLocal<ReadNodeDB> readNodeDB;
    protected final RocksDB nodes;

    public Storage(@NonNull File root) throws Exception {
        SQLiteDataSource dataSource = SqliteDB.dataSource(new File(root, "nodes.sqlite"));
        this.writeNodeDB = new WriteNodeDB(dataSource);
        this.readNodeDB = CloseableThreadLocal.of(() -> new ReadNodeDB(dataSource));

        this.nodes = RocksDB.open(PFiles.ensureDirectoryExists(new File(root, "nodes")).toString());
    }

    public ReadNodeDB readNodeDB() {
        return this.readNodeDB.get();
    }

    public void sync() throws Exception {
        this.writeNodeDB.commit();

        this.nodes.flushWal(true);
    }

    @Override
    public void close() throws Exception {
        this.writeNodeDB.close();
        this.readNodeDB.close();

        this.nodes.close();
    }
}
