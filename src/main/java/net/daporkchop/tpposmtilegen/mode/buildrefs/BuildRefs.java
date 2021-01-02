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

package net.daporkchop.tpposmtilegen.mode.buildrefs;

import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.natives.PolygonAssembler;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class BuildRefs implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        //System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "1");

        try (Storage storage = new Storage(Paths.get(args[0]))) {
            System.out.println("clearing references database");
            storage.references().clear();
            System.out.println("cleared.");

            try (ProgressNotifier notifier = new ProgressNotifier(" Build References: ", 5000L, "ways", "relations")) {
                StreamSupport.longStream(storage.wayFlags().spliterator(), true).forEach(id -> {
                    try {
                        notifier.step(0);
                        Way way = storage.ways().get(id);
                        checkArg(way != null, "unknown way: %d", id);
                        way.computeReferences(storage);
                    } catch (Exception e) {
                        throw new RuntimeException("way " + id, e);
                    }
                });
                StreamSupport.longStream(storage.relationFlags().spliterator(), true).forEach(id -> {
                    try {
                        notifier.step(1);
                        Relation relation = storage.relations().get(id);
                        checkArg(relation != null, "unknown relation: %d", id);
                        relation.computeReferences(storage);
                    } catch (Exception e) {
                        throw new RuntimeException("relation " + id, e);
                    }
                });
            }
        }
    }
}
