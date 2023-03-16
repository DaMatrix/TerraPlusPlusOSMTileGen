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

import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.function.exception.EFunction;
import net.daporkchop.lib.common.misc.Tuple;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.logging.LogAmount;
import net.daporkchop.lib.primitive.lambda.LongIntConsumer;
import net.daporkchop.tpposmtilegen.natives.Memory;
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.iterate.RocksColumnSpliterator;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Threading;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.Utils;
import org.rocksdb.ColumnFamilyHandle;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class VerifyMergeOpReferences {
    private static native void findAndPrintReferences(long begin, long end, long key);

    public static void main(String... args) throws Exception {
        logger.redirectStdOut().enableANSI()
                .addFile(new File("logs/" + Instant.now() + ".log"), LogAmount.DEBUG)
                .setLogAmount(LogAmount.DEBUG);

        try (//Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/planet-5-dictionary-zstd-compression/planet"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-4-aggressive-zstd-compression/planet"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-test/planet"), DatabaseConfig.RO_GENERAL);
             //Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/planet-test/planet-assembled-v3-compact-everything-constantly"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-test/planet-assembled-v2-compact-tiles-after-post-compaction"), DatabaseConfig.RO_GENERAL);
             Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/planet-test/planet-assembled-v3-updated-5456500-original-slow"), DatabaseConfig.RO_GENERAL);
             Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-test/planet-assembled-v3-updated-5456500-new-technique-2-very-fast"), DatabaseConfig.RO_GENERAL);

             //Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/switzerland-legacy"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/switzerland"), DatabaseConfig.RO_GENERAL);
        ) {
            //long properVersion = properStorage.sequenceNumber().get(properStorage.db().read());
            //long properVersion = properStorage.sequenceNumberProperty().getLong(properStorage.db().read()).getAsLong();
            //long testVersion = testStorage.sequenceNumberProperty().getLong(testStorage.db().read()).getAsLong();
            //checkState(properVersion == testVersion, "%d != %d", properVersion, testVersion);

            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Verify geometry")
                    .slot("tiles").slot("blobs")
                    .build();
                 DBReadAccess properReadAccess = properStorage.db().snapshot();
                 DBReadAccess testReadAccess = testStorage.db().snapshot()) {

                /*try (RocksColumnSpliterator spliterator = new RocksColumnSpliterator(
                        properStorage.db(), properStorage.db().internalColumnFamily(properStorage.externalJsonStorage()[0]),
                        Optional.empty(), DatabaseConfig.ReadType.BULK_ITERATE, RocksColumnSpliterator.KeyOperations.FIXED_SIZE_LEX_ORDER)) {
                    notifier.setTotal(0, 0L);
                    Threading.forEachParallel(PorkUtil.CPU_COUNT / 4, s -> s.forEachRemaining(slice -> notifier.incrementTotal(0)), spliterator);
                    //spliterator.forEachRemaining(slice -> notifier.incrementTotal(0));
                }
                try (RocksColumnSpliterator spliterator = new RocksColumnSpliterator(
                        testStorage.db(), testStorage.db().internalColumnFamily(testStorage.externalJsonStorage()[0]),
                        Optional.empty(), DatabaseConfig.ReadType.BULK_ITERATE, RocksColumnSpliterator.KeyOperations.FIXED_SIZE_LEX_ORDER)) {
                    Threading.forEachParallel(PorkUtil.CPU_COUNT / 4, s -> s.forEachRemaining(slice -> notifier.step(0)), spliterator);
                    //spliterator.forEachRemaining(slice -> notifier.step(0));
                }
                if (true) {
                    return;
                }*/

                Map<ColumnFamilyHandle, ColumnFamilyHandle> properToTestColumnFamilyHandles = Utils.zip(
                        Stream.of(properStorage.intersectedTiles(), properStorage.externalJsonStorage(), properStorage.tileJsonStorage())
                                .flatMap(Stream::of)
                                .map(properStorage.db()::internalColumnFamily),
                        Stream.of(testStorage.intersectedTiles(), testStorage.externalJsonStorage(), testStorage.tileJsonStorage())
                                .flatMap(Stream::of)
                                .map(testStorage.db()::internalColumnFamily))
                        .collect(Collectors.toMap(Tuple::getA, Tuple::getB));

                Threading.forEachParallel(PorkUtil.CPU_COUNT / 3,
                        (EConsumer<RocksColumnSpliterator>) properSpliterator -> {
                            try (DBIterator testIterator = testReadAccess.iterator(properToTestColumnFamilyHandles.get(properSpliterator.column()), properSpliterator.smallestKeyInclusive(), properSpliterator.largestKeyExclusive())) {
                                testIterator.seekToFirst();

                                properSpliterator.forEachRemaining(properSlice -> {
                                    checkState(testIterator.isValid());
                                    
                                    NativeRocksHelper.KeyValueSlice testSlice = testIterator.keyValueSlice();

                                    if (testSlice.keySize() != properSlice.keySize() || Memory.memcmp(properSlice.keyAddr(), testSlice.keyAddr(), testSlice.keySize()) != 0) {
                                        throw new IllegalStateException("key");
                                    }

                                    if (testSlice.valueSize() != properSlice.valueSize() || Memory.memcmp(properSlice.valueAddr(), testSlice.valueAddr(), testSlice.valueSize()) != 0) {
                                        throw new IllegalStateException("value");
                                    }

                                    testIterator.next();
                                    notifier.step(1);
                                });
                                checkState(!testIterator.isValid());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        },
                        Stream.of(properStorage.intersectedTiles(), properStorage.externalJsonStorage(), properStorage.tileJsonStorage())
                                .flatMap(Stream::of)
                                .map(properStorage.db()::internalColumnFamily)
                                .map((EFunction<ColumnFamilyHandle, RocksColumnSpliterator>) cf ->
                                        new RocksColumnSpliterator(properStorage.db(), cf, properReadAccess.internalSnapshot(), DatabaseConfig.ReadType.BULK_ITERATE, RocksColumnSpliterator.KeyOperations.FIXED_SIZE_LEX_ORDER))
                                .toArray(RocksColumnSpliterator[]::new));

                /*CompletableFuture<?> verifyBlobsTask = CompletableFuture.runAsync(() -> IntStream.range(0, Utils.MAX_LEVELS)
                        .parallel()
                        .boxed()
                        .forEach((EConsumer<Integer>) level -> {
                            try (DBIterator it1 = properStorage.db().read().iterator(properStorage.db().internalColumnFamily(properStorage.externalJsonStorage()[level]));
                                 DBIterator it2 = testStorage.db().read().iterator(testStorage.db().internalColumnFamily(testStorage.externalJsonStorage()[level]))) {
                                for (it1.seekToFirst(), it2.seekToFirst(); it1.isValid() && it2.isValid(); it1.next(), it2.next()) {
                                    if (!Arrays.equals(it1.key(), it2.key())) {
                                        throw new IllegalStateException("key");
                                    }
                                    if (!Arrays.equals(it1.value(), it2.value())) {
                                        throw new IllegalStateException("value");
                                    }
                                    notifier.step(1);
                                }
                                if (it1.isValid() != it2.isValid()) {
                                    throw new IllegalStateException("lengths differ");
                                }
                            }
                        }));

                IntStream.range(0, Utils.MAX_LEVELS).parallel().forEach(level -> Tile.levelTiles(level, true).forEach(tilePos -> {
                    try {
                        List<Long2ObjectMap.Entry<byte[]>> properValues = new ArrayList<>();
                        List<Long2ObjectMap.Entry<byte[]>> testValues = new ArrayList<>();
                        properStorage.tileJsonStorage()[level].getElementsInTile(properReadAccess, tilePos, (key, value) -> properValues.add(new AbstractLong2ObjectMap.BasicEntry<>(key, value)));
                        testStorage.tileJsonStorage()[level].getElementsInTile(testReadAccess, tilePos, (key, value) -> testValues.add(new AbstractLong2ObjectMap.BasicEntry<>(key, value)));

                        notifier.incrementTotal(0);
                        if (!properValues.isEmpty()) {
                            notifier.step(0);
                        }

                        if (properValues.size() != testValues.size()) {
                            throw new IllegalStateException("size mismatch");
                        }
                        for (int i = 0; i < properValues.size(); i++) {
                            Long2ObjectMap.Entry<byte[]> properValue = properValues.get(i);
                            Long2ObjectMap.Entry<byte[]> testValue = testValues.get(i);
                            if (properValue.getLongKey() != testValue.getLongKey() || !Arrays.equals(properValue.getValue(), testValue.getValue())) {
                                throw new IllegalStateException("value mismatch at #" + i);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(PStrings.fastFormat("tile (%d, %d) at level %d", Tile.tileX(tilePos), Tile.tileY(tilePos), level), e);
                    }
                }));

                verifyBlobsTask.join();*/

                if (true) {
                    return;
                }
            }

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
                //properStorage.ways().forEachKeyParallel(properReadAccess, id -> func.accept(id, Way.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);
                //properStorage.relations().forEachKeyParallel(properReadAccess, id -> func.accept(id, Relation.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);
                //properStorage.coastlines().forEachKeyParallel(properReadAccess, id -> func.accept(id, Coastline.TYPE), RocksDBMap.KeyDistribution.EVEN_DENSE);

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
