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
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.osm.Updater;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;

import java.io.File;
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
        checkArg(args.length == 3, "Usage: serve_with_updates <index_dir> <port> <lite>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        boolean lite = Boolean.parseBoolean(args[2]);

        try (Storage storage = new Storage(src.toPath(), lite ? DatabaseConfig.RW_LITE : DatabaseConfig.RW_GENERAL);
             Serve.Server server = new Serve.Server(Integer.parseUnsignedInt(args[1]), storage, storage.db().read())) {
            AtomicBoolean running = new AtomicBoolean(true);
            Thread updateThread = new Thread((ERunnable) () -> {
                while (running.get()) {
                    logger.info("Checking for updates...");
                    Updater updater = new Updater(storage);
                    int updateCount = 0;

                    boolean[] result = new boolean[1];
                    do {
                        Thread thread = new FastThreadLocalThread((ERunnable) () -> {
                            try (DBAccess txn = storage.db().newTransaction()) {
                                result[0] = updater.update(storage, txn);
                                txn.flush(); //commit changes
                            }
                        });

                        thread.start();
                        try {
                            thread.join();
                        } catch (InterruptedException ignored) {
                            thread.join();
                            result[0] = false;
                        }
                        updateCount++;
                    } while (running.get() && result[0]);

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
            });
            updateThread.start();

            new Scanner(System.in).nextLine();
            logger.info("Waiting for update thread to stop...");
            running.set(false);
            updateThread.interrupt();
            updateThread.join();
        }
    }
}
