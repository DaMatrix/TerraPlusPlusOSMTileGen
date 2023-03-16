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

import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.NonNull;
import net.daporkchop.lib.common.function.exception.ERunnable;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.osm.Updater;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.util.Utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class ServeWithUpdates implements IMode {
    @Override
    public String name() {
        return "serve_with_updates";
    }

    @Override
    public String synopsis() {
        return "<index_dir> <port> <lite>";
    }

    @Override
    public String help() {
        return "Launches a web server which serves the tiles locally, and also applies minutely updates.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        Utils.setAllowForkJoinPool();

        checkArg(args.length == 3, "Usage: serve_with_updates <index_dir> <port> <lite>");
        Path src = PFiles.assertDirectoryExists(Paths.get(args[0]));
        boolean lite = Boolean.parseBoolean(args[2]);

        try (Storage storage = new Storage(src, lite ? DatabaseConfig.RW_LITE : DatabaseConfig.RW_GENERAL);
             Serve.Server server = new Serve.Server(Integer.parseUnsignedInt(args[1]), storage, storage.db().read())) {
            AtomicBoolean running = new AtomicBoolean(true);
            Thread updateThread = new FastThreadLocalThread((ERunnable) () -> {
                while (running.get()) {
                    logger.info("Checking for updates...");
                    Updater updater = new Updater(storage);
                    int updateCount = 0;

                    boolean result;
                    do {
                        try (DBAccess txn = storage.db().newTransaction()) {
                            result = updater.update(storage, txn);
                            txn.flush(); //commit changes
                        }
                        updateCount++;
                    } while (running.get() && result);

                    if (updateCount == 0) {
                        logger.info("No updates found.");
                    } else {
                        logger.info("Processed %d changesets.", updateCount);
                    }
                    if (running.get()) {
                        try {
                            Thread.sleep(TimeUnit.MINUTES.toMillis(1L));
                        } catch (InterruptedException ignored) {
                            //exit silently
                        }
                    }
                }
            }, "Update thread");
            updateThread.start();

            new Scanner(System.in).nextLine();
            logger.info("Waiting for update thread to stop...");
            running.set(false);
            updateThread.interrupt();
            updateThread.join();
        }
    }
}
