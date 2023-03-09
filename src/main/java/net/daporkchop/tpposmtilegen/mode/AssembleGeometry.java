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
import net.daporkchop.tpposmtilegen.natives.UInt64SetUnsortedWriteAccess;
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
import java.util.IdentityHashMap;
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
            UInt64BlobUnsortedWriteAccess[] externalJsonStorageAccesses = Stream.of(storage.externalJsonStorage())
                    .map((EFunction<WrappedRocksDB, UInt64BlobUnsortedWriteAccess>) externalJsonStorageLevel -> new UInt64BlobUnsortedWriteAccess(
                            storage, storage.db().internalColumnFamily(externalJsonStorageLevel), 0.000233935708d))
                    .toArray(UInt64BlobUnsortedWriteAccess[]::new);

            UInt64BlobUnsortedWriteAccess[] intersectedTilesAccesses = Stream.of(storage.intersectedTiles())
                    .map((EFunction<WrappedRocksDB, UInt64BlobUnsortedWriteAccess>) intersectedTilesLevel -> new UInt64BlobUnsortedWriteAccess(
                            storage, storage.db().internalColumnFamily(intersectedTilesLevel), 0.054970339304d))
                    .toArray(UInt64BlobUnsortedWriteAccess[]::new);

            try (WriteRedirectingAccess access = new WriteRedirectingAccess(
                         storage.db().read(),
                         WriteRedirectingAccess.indexWriteDelegates(
                                 Stream.of(externalJsonStorageAccesses, intersectedTilesAccesses)
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
                        storage.convertToGeoJSONAndStoreInDB(access, Element.addTypeToId(type, id), element, false, true);
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                };

                //storage.nodes().forEachParallel(storage.db().read(), func);
                //storage.ways().forEachParallel(storage.db().read(), func);
                storage.relations().forEachParallel(storage.db().read(), func);
                storage.coastlines().forEachParallel(storage.db().read(), func);
            }

            Stream.of(externalJsonStorageAccesses, intersectedTilesAccesses).flatMap(Stream::of).forEach((EConsumer<DBWriteAccess>) DBWriteAccess::close);

            if (true) {
                List<WrappedRocksDB> toCompact = Stream.of(storage.intersectedTiles(), storage.tileJsonStorage(), storage.externalJsonStorage())
                        .flatMap(Stream::of).collect(Collectors.toList());
                for (WrappedRocksDB column : toCompact) {
                    try (TimedOperation compactOperation = new TimedOperation(
                            new String(storage.db().internalColumnFamily(column).getName(), StandardCharsets.UTF_8) + " Compaction")) {
                        column.compact();
                    }
                }
            } else {
                try (TimedOperation compactOperation = new TimedOperation("Compaction")) {
                    List<WrappedRocksDB> toCompact = Stream.of(storage.intersectedTiles(), storage.tileJsonStorage(), storage.externalJsonStorage())
                            .flatMap(Stream::of).collect(Collectors.toList());

                    Threading.iterateParallel(toCompact.size(), toCompact.size(), toCompact::forEach, WrappedRocksDB::compact);
                }
            }
        }
    }
}
