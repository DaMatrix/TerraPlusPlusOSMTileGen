/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2021 DaPorkchop_
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
import net.daporkchop.lib.common.function.PFunctions;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.common.function.throwing.ESupplier;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.threadfactory.PThreadFactories;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Threading;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class RebuildPlanet implements IMode {
    private static void nukeTileDirectory(@NonNull Path dir) throws Exception {
        final int threadCount = 32; //SATA has a command buffer size of 32

        try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Nuke tile directory")
                .slot("files").slot("directories")
                .build()) {
            List<Thread> threads = new ArrayList<>(threadCount);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount, r -> {
                Thread thread = PThreadFactories.DEFAULT_THREAD_FACTORY.newThread(r);
                threads.add(thread);
                return thread;
            });

            _enumerateFilesRecursive(executor, notifier, dir).join();
            _deleteFilesRecursive(executor, notifier, dir).join();

            executor.shutdown();
            threads.forEach((EConsumer<Thread>) Thread::join);
        }
    }

    private static CompletableFuture<Void> _enumerateFilesRecursive(@NonNull ExecutorService executor, @NonNull ProgressNotifier notifier, @NonNull Path path) throws Exception {
        if (Files.isDirectory(path)) {
            notifier.incrementTotal(1);
            try (Stream<Path> stream = Files.list(path)) {
                return CompletableFuture.allOf(stream
                        .map(p -> CompletableFuture.supplyAsync((ESupplier<CompletableFuture<Void>>) () -> _enumerateFilesRecursive(executor, notifier, p), executor)
                                .thenCompose(PFunctions.identity()))
                        .toArray(CompletableFuture[]::new));
            }
        } else {
            notifier.incrementTotal(0);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static CompletableFuture<Void> _deleteFilesRecursive(@NonNull ExecutorService executor, @NonNull ProgressNotifier notifier, @NonNull Path path) throws Exception {
        if (Files.isDirectory(path)) {
            try (Stream<Path> stream = Files.list(path)) {
                return CompletableFuture.allOf(stream
                        .map(p -> CompletableFuture.supplyAsync((ESupplier<CompletableFuture<Void>>) () -> _deleteFilesRecursive(executor, notifier, p), executor)
                                .thenCompose(PFunctions.identity()))
                        .toArray(CompletableFuture[]::new))
                        .thenRun((ERunnable) () -> {
                            Files.delete(path);
                            notifier.step(1);
                        });
            }
        } else {
            Files.delete(path);
            notifier.step(0);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public String name() {
        return "rebuild_planet";
    }

    @Override
    public String synopsis() {
        return "<index_dir> <tile_dir>";
    }

    @Override
    public String help() {
        return "Regenerates all tiles.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: rebuild_planet <index_dir> <tile_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        Path dst = Paths.get(args[1]);
        if (PFiles.checkDirectoryExists(dst.toFile())) {
            nukeTileDirectory(dst);
        }

        try (Storage storage = new Storage(src.toPath())) {
            storage.purge(true, true); //clear everything

            logger.info("Optimizing point DB...");
            storage.points().optimize();
            logger.info("Optimization complete.");

            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Assemble & index geometry")
                    .slot("nodes").slot("ways").slot("relations").slot("coastlines", storage.coastlineCount().get())
                    .build()) {
                CompletableFuture.allOf(
                        CompletableFuture.runAsync(() -> notifier.setTotal(0, StreamSupport.longStream(storage.taggedNodeFlags().spliterator(), false).count())),
                        CompletableFuture.runAsync(() -> notifier.setTotal(1, StreamSupport.longStream(storage.wayFlags().spliterator(), false).count())),
                        CompletableFuture.runAsync(() -> notifier.setTotal(2, StreamSupport.longStream(storage.relationFlags().spliterator(), false).count()))
                ).join();

                Threading.forEachParallelLong(combinedId -> {
                    int type = Element.extractType(combinedId);
                    try {
                        storage.convertToGeoJSONAndStoreInDB(storage.db().batch(), dst, combinedId);
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + Element.extractId(combinedId), e);
                    }
                    notifier.step(type);
                }, storage.spliterateElements(false, true, true, true));
                storage.flush();
            }

            storage.exportDirtyTiles(storage.db().read(), dst);

            storage.purge(true, false); //erase temporary data
        }
    }
}
