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
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.lib.common.function.throwing.EFunction;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.threadfactory.PThreadFactories;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class Compact implements IMode {
    private static CompletableFuture<Void> run(@NonNull Executor executor, @NonNull WrappedRocksDB db, @NonNull String name) {
        return CompletableFuture.runAsync((ERunnable) db::optimize, executor).thenRun(() -> logger.info("Compacted %s.", name));
    }

    @Override
    public String name() {
        return "compact";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "Runs RocksDB compaction on the index's databases.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: compact <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        logger.info("Opening storage...");
        try (Storage storage = new Storage(src.toPath())) {
            logger.info("Running compaction...");

            List<Thread> threads = new ArrayList<>();
            ExecutorService executor = Executors.newCachedThreadPool(r -> {
                Thread t = PThreadFactories.DEFAULT_THREAD_FACTORY.newThread(r);
                threads.add(t);
                return t;
            });

            CompletableFuture.allOf(
                    Arrays.stream(Storage.class.getDeclaredFields())
                            .filter(f -> WrappedRocksDB.class.isAssignableFrom(f.getType()))
                            .peek(f -> f.setAccessible(true))
                            .map((EFunction<Field, CompletableFuture<Void>>)
                                    f -> run(executor, (WrappedRocksDB) f.get(storage), f.getName()))
                            .toArray(CompletableFuture[]::new)
            ).join();

            logger.info("Wrapping up...");
            executor.shutdown();
            threads.forEach((EConsumer<Thread>) Thread::join);
        }
        logger.success("Done.");
    }
}
