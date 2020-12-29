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

package net.daporkchop.tpposmtilegen.mode.testindex;

import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.io.File;
import java.sql.ResultSet;
import java.util.concurrent.ThreadLocalRandom;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class TestIndex implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        PFiles.rmContents(new File(args[0]));
        try (Storage storage = new Storage(Storage.dataSource(new File(args[0])))) {
            storage.putNode().setLong(1, 0L);
            storage.putNode().setDouble(2, 7.0d);
            storage.putNode().setDouble(3, 47.0d);
            storage.putNode().setBytes(4, null);
            storage.putNode().addBatch();

            storage.putNode().setLong(1, 8L);
            storage.putNode().setDouble(2, 8.0d);
            storage.putNode().setDouble(3, 47.5d);
            storage.putNode().setBytes(4, null);
            storage.putNode().addBatch();

            storage.putNode().executeBatch();

            System.out.println("state #0");
            try (ResultSet results = storage.getAllNodes().executeQuery()) {
                while (results.next()) {
                    System.out.println(storage.nodeToString(results));
                }
            }

            storage.putNode().setLong(1, 0L);
            storage.putNode().setDouble(2, 2.0d);
            storage.putNode().setDouble(3, 3.0d);
            storage.putNode().setBytes(4, null);
            storage.putNode().addBatch();

            storage.putNode().setLong(1, 0L);
            storage.putNode().setDouble(2, 4.0d);
            storage.putNode().setDouble(3, 5.0d);
            storage.putNode().setBytes(4, null);
            storage.putNode().addBatch();

            storage.putNode().executeBatch();

            System.out.println("state #1");
            try (ResultSet results = storage.getAllNodes().executeQuery()) {
                while (results.next()) {
                    System.out.println(storage.nodeToString(results));
                }
            }

            System.out.println("nodes with id 8");
            storage.getNodeById().setLong(1, 8L);
            try (ResultSet results = storage.getNodeById().executeQuery()) {
                while (results.next()) {
                    System.out.println(storage.nodeToString(results));
                }
            }

            for (int j = 0; j < 5; j++) {
                System.out.println("adding 1 million entries");
                for (int i = 0; i < 1000000; i++) {
                    storage.putNode().setLong(1, ThreadLocalRandom.current().nextLong());
                    storage.putNode().setDouble(2, ThreadLocalRandom.current().nextDouble());
                    storage.putNode().setDouble(3, ThreadLocalRandom.current().nextDouble());
                    storage.putNode().setBytes(4, null);
                    storage.putNode().addBatch();
                }

                System.out.println("executing");
                storage.putNode().executeBatch();
                System.out.println("committing");
                storage.commit();

                System.out.println("state after add:");
                try (ResultSet results = storage.countNodes().executeQuery()) {
                    checkState(results.next());
                    System.out.println(results.getLong(1));
                }
            }
        }
    }
}
