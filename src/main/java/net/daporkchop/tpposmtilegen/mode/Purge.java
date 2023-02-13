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
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.util.TimedOperation;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class Purge implements IMode {
    public static void purge(@NonNull Storage storage, @NonNull DataType... types) throws Exception {
        if (types.length == 0) {
            return;
        }

        try (TimedOperation purgeOperation = new TimedOperation("Purge")) {
            try (TimedOperation flushOperation = purgeOperation.pushChild("Flush DB")) {
                storage.flush();
            }

            try (TimedOperation clearOperation = purgeOperation.pushChild("Clearing DB columns")) {
                storage.db().clear(Stream.of(types).flatMap(type -> type.wrappersToClear(storage)).collect(Collectors.toList()));
            }
        }
    }

    @Override
    public String name() {
        return "purge";
    }

    @Override
    public String synopsis() {
        return "<index_dir> <data_type>...";
    }

    @Override
    public String help() {
        return "Deletes all data of the requested types.\n"
               + "Valid data types:\n"
               + "  - 'coastlines': Deletes all digested coastline data\n"
               + "  - 'geometry': Deletes all assembled geometry (both OSM and coastlines)\n"
               + "  - 'osm': Deletes all digested OSM data (including updates and update tracking information)";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length >= 1, "Usage: purge <index_dir> [<data_type>...]");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        DataType[] types;
        if (args.length == 1) {
            logger.info("Enter the data types to purge (separated by spaces).\nValid options: " + Arrays.toString(DataType.values()));
            try (Scanner sc = new Scanner(System.in)) {
                types = Stream.of(sc.nextLine().trim().split(" ")).filter(s -> !s.isEmpty()).map(DataType::valueOf).distinct().toArray(DataType[]::new);
                checkArg(types.length > 0, "at least one data type must be given!");
            }
        } else {
            types = Arrays.stream(args, 1, args.length).map(DataType::valueOf).distinct().toArray(DataType[]::new);
        }

        try (Storage storage = new Storage(src.toPath(), Database.DB_OPTIONS_LITE)) {
            purge(storage, types);
        }
    }

    public enum DataType {
        coastlines {
            @Override
            public Stream<? extends WrappedRocksDB> wrappersToClear(@NonNull Storage storage) {
                return Stream.of(storage.coastlines());
            }
        },
        geometry {
            @Override
            public Stream<? extends WrappedRocksDB> wrappersToClear(@NonNull Storage storage) {
                return Stream.of(
                        storage.intersectedTiles(),
                        storage.tileJsonStorage(),
                        storage.externalJsonStorage()
                ).flatMap(Stream::of);
            }
        },
        osm {
            @Override
            public Stream<? extends WrappedRocksDB> wrappersToClear(@NonNull Storage storage) {
                return Stream.of(
                        storage.nodes(),
                        storage.points(),
                        storage.ways(),
                        storage.relations(),
                        storage.references(),
                        storage.sequenceNumber()
                );
            }
        };

        protected abstract Stream<? extends WrappedRocksDB> wrappersToClear(@NonNull Storage storage);
    }
}
