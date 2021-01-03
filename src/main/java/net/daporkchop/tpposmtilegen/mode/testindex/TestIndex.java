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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.UInt64AddOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class TestIndex implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        //System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "1");

        long start = System.currentTimeMillis();
        try (Storage storage = new Storage(Paths.get(args[0]))) {
            //System.out.println("nodes: " + StreamSupport.longStream(storage.nodeFlags().spliterator(), true).count());

            if (false) {
                try (ProgressNotifier notifier = new ProgressNotifier(" found ", 500L, "ways", "and areas")) {
                    System.out.println("ways that are also areas:" + StreamSupport.longStream(storage.wayFlags().spliterator(), true)
                            .mapToObj(id -> {
                                try {
                                    Way way = storage.ways().get(id);
                                    checkState(way != null, "unknown way with id: %d", id);

                                    notifier.step(0);

                                    return way.toGeometry(storage);
                                } catch (Exception e) {
                                    RuntimeException re = new RuntimeException(String.valueOf(id), e);
                                    re.printStackTrace();
                                    throw re;
                                }
                            })
                            .filter(Objects::nonNull)
                            .peek(a -> notifier.step(1))
                            .count());
                }
            }

            if (false) {
                try (ProgressNotifier notifier = new ProgressNotifier(" found ", 500L, "relations", "and areas")) {
                    System.out.println("relations that are also areas:" + StreamSupport.longStream(storage.relationFlags().spliterator(), false)
                            .mapToObj(id -> {
                                try {
                                    Relation relation = storage.relations().get(id);
                                    checkState(relation != null, "unknown relation with id: %d", id);

                                    notifier.step(0);

                                    return relation.toGeometry(storage);
                                } catch (Exception e) {
                                    RuntimeException re = new RuntimeException(String.valueOf(id), e);
                                    re.printStackTrace();
                                    System.exit(1);
                                    throw re;
                                }
                            })
                            .filter(Objects::nonNull)
                            .peek(a -> notifier.step(1))
                            .count());
                }
            }

            if (false) {
                StreamSupport.longStream(storage.nodeFlags().spliterator(), true)
                        .forEach(id -> {
                            try {
                                LongList dst = new LongArrayList();
                                storage.references().getReferencesTo(Node.TYPE, id, dst);
                                if (dst.isEmpty()) {
                                    System.out.printf("node %d is referenced 0 times\n", id);
                                } else {
                                    System.out.printf("node %d is referenced %d times: %s\n", id, dst.size(), dst);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(String.valueOf(id), e);
                            }
                        });
            }
        }
        long end = System.currentTimeMillis();
        System.out.printf(" took %.2fs\n", (end - start) / 1000.0d);
    }
}
