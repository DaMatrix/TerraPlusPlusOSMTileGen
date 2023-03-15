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

package net.daporkchop.tpposmtilegen.mode;

import lombok.NonNull;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.function.exception.EFunction;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.tpposmtilegen.natives.AbstractUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.natives.UInt64BlobUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.natives.UInt64ToBlobMapUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WriteRedirectingAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Threading;
import net.daporkchop.tpposmtilegen.util.TimedOperation;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class AssembleGeometry implements IMode {
    @Override
    public String name() {
        return "assemble_geometry";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "Assembles and indexes all geometry elements.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: assemble_geometry <index_dir>");
        Path src = PFiles.assertDirectoryExists(Paths.get(args[0]));

        try (Storage storage = new Storage(src, DatabaseConfig.RW_LITE_BULK_LOAD)) {
            Purge.purge(storage, Purge.DataType.geometry);
        }

        try (Storage storage = new Storage(src, DatabaseConfig.RW_BULK_LOAD)) {
            AbstractUnsortedWriteAccess[] externalJsonStorageAccesses = Stream.of(storage.externalJsonStorage())
                    //.filter(i -> false)
                    .map((EFunction<WrappedRocksDB, AbstractUnsortedWriteAccess>) externalJsonStorageLevel -> new UInt64BlobUnsortedWriteAccess(
                            storage, storage.db().internalColumnFamily(externalJsonStorageLevel), 0.000440108682))
                    .toArray(AbstractUnsortedWriteAccess[]::new);

            AbstractUnsortedWriteAccess[] intersectedTilesAccesses = Stream.of(storage.intersectedTiles())
                    //.filter(i -> false)
                    .map((EFunction<WrappedRocksDB, AbstractUnsortedWriteAccess>) intersectedTilesLevel -> new UInt64BlobUnsortedWriteAccess(
                            storage, storage.db().internalColumnFamily(intersectedTilesLevel), 0.206963215028d))
                    .toArray(AbstractUnsortedWriteAccess[]::new);

            AbstractUnsortedWriteAccess[] tileJsonStorageAccesses = Stream.of(storage.tileJsonStorage())
                    .peek((EConsumer<WrappedRocksDB>) db -> storage.db().delegate().enableAutoCompaction(Collections.singletonList(storage.db().internalColumnFamily(db))))
                    .filter(i -> false)
                    .map((EFunction<WrappedRocksDB, AbstractUnsortedWriteAccess>) tileJsonStorageLevel -> new UInt64ToBlobMapUnsortedWriteAccess(
                            storage, storage.db().internalColumnFamily(tileJsonStorageLevel), 5.754273128207d))
                    .toArray(AbstractUnsortedWriteAccess[]::new);

            try (WriteRedirectingAccess access = new WriteRedirectingAccess(
                         storage.db().read(),
                         WriteRedirectingAccess.indexWriteDelegates(
                                 Stream.of(externalJsonStorageAccesses, intersectedTilesAccesses, tileJsonStorageAccesses)
                                         .flatMap(Stream::of)
                                         .flatMap(specificAccess -> Stream.of(specificAccess.columnFamilyHandle(), specificAccess))
                                         .toArray()),
                         storage.db().batch());
                 ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Assemble Geometry")
                         .slot("nodes", 0L).slot("ways", 0L).slot("relations", 0L).slot("coastlines", 0L)
                         .build()) {
                LongObjConsumer<Element> func = (id, element) -> {
                    int type = element.type();
                    notifier.incrementTotal(type);
                    if (!element.visible()) {
                        return;
                    }
                    notifier.step(type);

                    try {
                        storage.convertToGeoJSONAndStoreInDB(access, Element.addTypeToId(type, id), null, element);
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                };

                //storage.nodes().forEachParallel(storage.db().read(), func);
                storage.ways().forEachParallel(storage.db().read(), func);
                storage.relations().forEachParallel(storage.db().read(), func);
                storage.coastlines().forEachParallel(storage.db().read(), func);
            }

            Stream.of(externalJsonStorageAccesses, intersectedTilesAccesses, tileJsonStorageAccesses).flatMap(Stream::of).forEach((EConsumer<DBWriteAccess>) DBWriteAccess::close);

            for (WrappedRocksDB column : Stream.of(storage.intersectedTiles(), storage.tileJsonStorage(), storage.externalJsonStorage())
                    .flatMap(Stream::of).collect(Collectors.toList())) {
                try (TimedOperation compactOperation = new TimedOperation(
                        new String(storage.db().internalColumnFamily(column).getName(), StandardCharsets.UTF_8) + " Compaction")) {
                    column.compact();
                }
            }
        }
    }
}
