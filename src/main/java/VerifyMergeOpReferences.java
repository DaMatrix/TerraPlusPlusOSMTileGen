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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongList;
import net.daporkchop.lib.logging.LogAmount;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class VerifyMergeOpReferences {
    public static void main(String... args) throws Exception {
        logger.redirectStdOut().enableANSI()
                .addFile(new File("logs/" + Instant.now() + ".log"), LogAmount.DEBUG)
                .setLogAmount(LogAmount.DEBUG);

        try (//Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/planet-5-dictionary-zstd-compression/planet"), DatabaseConfig.RO_GENERAL);
             //Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/planet-4-aggressive-zstd-compression/planet"), DatabaseConfig.RO_GENERAL);
             Storage properStorage = new Storage(Paths.get("/media/daporkchop/data/switzerland-reference"), DatabaseConfig.RO_GENERAL);
             Storage testStorage = new Storage(Paths.get("/media/daporkchop/data/switzerland"), DatabaseConfig.RO_GENERAL);
        ) {
            long properVersion = properStorage.sequenceNumber().get(properStorage.db().read());
            long testVersion = testStorage.sequenceNumber().get(testStorage.db().read());
            checkState(properVersion == testVersion, "%d != %d", properVersion, testVersion);

            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Recompute references")
                    .slot("nodes").slot("ways").slot("relations").slot("coastlines").slot("references")
                    .build()) {
                LongObjConsumer<Element> func = (id, element) -> {
                    int type = element.type();
                    try {
                        LongList properReferences = new LongArrayList();
                        LongList testReferences = new LongArrayList();
                        properStorage.references().getReferencesTo(properStorage.db().read(), Element.addTypeToId(type, id), properReferences);
                        testStorage.references().getReferencesTo(testStorage.db().read(), Element.addTypeToId(type, id), testReferences);
                        if (!properReferences.equals(testReferences)) {
                            throw new IllegalStateException(properReferences + " != " + testReferences);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                    notifier.step(type);
                };

                //storage.nodes().forEachParallel(storage.db().read(), func);
                //properStorage.ways().forEachParallel(properStorage.db().read(), func);
                //properStorage.relations().forEachParallel(properStorage.db().read(), func);
                //properStorage.coastlines().forEachParallel(properStorage.db().read(), func);

                if (true) {
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
                } else {
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
