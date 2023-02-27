/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
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

package net.daporkchop.tpposmtilegen;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.daporkchop.lib.common.math.PMath;
import net.daporkchop.lib.logging.LogAmount;
import net.daporkchop.lib.primitive.lambda.LongIntConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.Natives;
import net.daporkchop.tpposmtilegen.natives.UInt64SetUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.osm.Coastline;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.map.RocksDBMap;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class VerifyMergeOpReferences {
    private static native void findAndPrintReferences(long begin, long end, long key);

    public static void main(String... args) throws Exception {
        /*if (false) {
            PUnsafe.ensureClassInitialized(Natives.class);
            try (FileChannel channel = FileChannel.open(Paths.get("/media/daporkchop/data/planet-test/references-sorted.buf"), StandardOpenOption.READ);
                 MemoryMap mmap = new MemoryMap(channel, FileChannel.MapMode.READ_ONLY, 0L, channel.size())) {
                findAndPrintReferences(mmap.addr(), mmap.addr() + mmap.size(), Element.addTypeToId(Way.TYPE, 78529221L));

                final long targetBlockSize = PMath.roundUp((long) (4.266666667d * (65536L << 10L)), 16L);
                UInt64SetUnsortedWriteAccess.partitionSortedRange(mmap.addr(), mmap.size(), targetBlockSize);
            }
            return;
        }*/

        logger.redirectStdOut().enableANSI()
                .addFile(new File("logs/" + Instant.now() + ".log"), LogAmount.DEBUG)
                .setLogAmount(LogAmount.DEBUG);

        try (Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/planet-5-dictionary-zstd-compression/planet"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-4-aggressive-zstd-compression/planet"), DatabaseConfig.RO_GENERAL);
             Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-test/planet"), DatabaseConfig.RO_GENERAL);

             //Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/switzerland-reference"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/switzerland"), DatabaseConfig.RO_GENERAL);
        ) {
            long properVersion = properStorage.sequenceNumber().get(properStorage.db().read());
            //long properVersion = properStorage.sequenceNumberProperty().getLong(properStorage.db().read()).getAsLong();
            long testVersion = testStorage.sequenceNumberProperty().getLong(testStorage.db().read()).getAsLong();
            checkState(properVersion == testVersion, "%d != %d", properVersion, testVersion);

            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Verify references")
                    .slot("nodes").slot("ways").slot("relations").slot("coastlines").slot("references")
                    .build();
                 DBReadAccess properReadAccess = properStorage.db().snapshot();
                 DBReadAccess testReadAccess = testStorage.db().snapshot()) {
                LongIntConsumer func = (id, type) -> {
                    try {
                        LongList properReferences = new LongArrayList();
                        LongList testReferences = new LongArrayList();
                        properStorage.references().getReferencesTo(properReadAccess, Element.addTypeToId(type, id), properReferences);
                        testStorage.references().getReferencesTo(testReadAccess, Element.addTypeToId(type, id), testReferences);
                        if (!properReferences.equals(testReferences)) {
                            Function<LongList, String> mapper = list -> list.stream()
                                    .map(combinedId -> Element.typeName(Element.extractType(combinedId)) + ' ' + Element.extractId(combinedId))
                                    .collect(Collectors.joining(", ", "[", "]"));
                            throw new IllegalStateException(mapper.apply(properReferences) + " != " + mapper.apply(testReferences));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                    notifier.step(type);
                };

                //properStorage.nodes().forEachKeyParallel(properReadAccess, id -> func.accept(id, Node.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);
                properStorage.ways().forEachKeyParallel(properReadAccess, id -> func.accept(id, Way.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);
                properStorage.relations().forEachKeyParallel(properReadAccess, id -> func.accept(id, Relation.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);
                properStorage.coastlines().forEachKeyParallel(properReadAccess, id -> func.accept(id, Coastline.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);

                if (false) {
                    try (DBIterator it1 = properStorage.db().read().iterator(properStorage.db().internalColumnFamily(properStorage.references()));
                         DBIterator it2 = testStorage.db().read().iterator(testStorage.db().internalColumnFamily(testStorage.references()))) {
                        for (it1.seekToFirst(), it2.seekToFirst(); it1.isValid() && it2.isValid(); it1.next(), it2.next()) {
                            if (!Arrays.equals(it1.key(), it2.key())) {
                                throw new IllegalStateException("key");
                            }
                            if (!Arrays.equals(it1.value(), it2.value())) {
                                throw new IllegalStateException("value");
                            }
                            notifier.step(4);
                        }
                        if (it1.isValid() != it2.isValid()) {
                            throw new IllegalStateException("lengths differ");
                        }
                    }
                } else if (false) {
                    properStorage.references().forEachKey(properStorage.db().read(), combinedId -> {
                        try {
                            LongList properReferences = new LongArrayList();
                            LongList testReferences = new LongArrayList();
                            properStorage.references().getReferencesTo(properStorage.db().read(), combinedId, properReferences);
                            testStorage.references().getReferencesTo(testStorage.db().read(), combinedId, testReferences);
                            if (!properReferences.equals(testReferences)) {
                                throw new IllegalStateException(Element.typeName(Element.extractType(combinedId)) + ' ' + Element.extractId(combinedId) + ": " +
                                                                properReferences + " != " + testReferences);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(Element.typeName(Element.extractType(combinedId)) + ' ' + Element.extractId(combinedId), e);
                        }
                        notifier.step(4);
                    });
                }
            }
        }
    }
}