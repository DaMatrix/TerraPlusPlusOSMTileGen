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
import net.daporkchop.tpposmtilegen.osm.Updater;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class Update implements IMode {
    @Override
    public String name() {
        return "update";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "Updates the index and tiles by applying the latest changesets from the OpenStreetMap database.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1 || args.length == 2, "Usage: update <index_dir> [lite=true]");
        Path src = PFiles.assertDirectoryExists(Paths.get(args[0]));
        boolean lite = args.length == 1 || Boolean.parseBoolean(args[1]);

        try (Storage storage = new Storage(src, lite ? DatabaseConfig.RW_LITE : DatabaseConfig.RW_GENERAL);
             DBAccess txn = storage.db().newTransaction()) {
            Updater updater = new Updater(storage);
            try (Scanner scanner = new Scanner(System.in)) {
                LOOP:
                while (true) {
                    String command = scanner.nextLine();
                    String[] split = command.split(" ");
                    switch (split[0]) {
                        case "h":
                        case "help":
                        case "?":
                            logger.info("Available commands:\n"
                                        + "  'info'\n"
                                        + "  'update'\n"
                                        + "  'update_to <target_sequence_number>'\n"
                                        + "  'commit'\n"
                                        + "  'rollback'\n"
                                        + "  'stop'");
                            break;
                        case "info":
                            if (split.length != 1) {
                                logger.warn("command '%s' expects no arguments", split[0]);
                                break;
                            }

                            logger.info("Database state: %s",
                                    storage.getChangesetState(storage.sequenceNumberProperty().getLong(storage.db().read()).getAsLong(), null).join());
                            logger.info("Current state:  %s (%s)",
                                    storage.getChangesetState(storage.sequenceNumberProperty().getLong(txn).getAsLong(), null).join(),
                                    storage.sequenceNumberProperty().getLong(txn).getAsLong() == storage.sequenceNumberProperty().getLong(storage.db().read()).getAsLong()
                                            ? "no changes" : "uncommitted");
                            logger.info("Latest state:   %s", storage.getLatestChangesetState().join());
                            break;
                        case "update":
                            if (split.length != 1) {
                                logger.warn("command '%s' expects no arguments", split[0]);
                                break;
                            }

                            logger.info("update result: %b", updater.update(storage, txn));
                            break;
                        case "update_to": {
                            if (split.length != 2) {
                                logger.warn("command '%s' expects one argument: <target_sequence_number>", split[0]);
                                break;
                            }

                            long targetSequenceNumber;
                            try {
                                targetSequenceNumber = Long.parseLong(split[1]);
                            } catch (NumberFormatException e) {
                                logger.warn("unparseable sequence number: %s", split[1]);
                                break;
                            }

                            if (storage.sequenceNumberProperty().getLong(txn).getAsLong() >= targetSequenceNumber) {
                                logger.warn("already at sequence number %d, cannot go back in time to reach sequence number %d!",
                                        storage.sequenceNumberProperty().getLong(txn).getAsLong(), targetSequenceNumber);
                                break;
                            }

                            do {
                                if (!updater.update(storage, txn)) {
                                    logger.warn("updater returned false, sequence number %d isn't available yet! stopping at %d",
                                            targetSequenceNumber, storage.sequenceNumberProperty().getLong(txn).getAsLong());
                                    break;
                                }
                            } while (storage.sequenceNumberProperty().getLong(txn).getAsLong() < targetSequenceNumber);
                            logger.info("update_to %d: done.", targetSequenceNumber);
                            break;
                        }
                        case "commit":
                            if (split.length != 1) {
                                logger.warn("command '%s' expects no arguments", split[0]);
                                break;
                            }

                            logger.info("Committing...");
                            txn.flush();
                            logger.success("Committed.");
                            break;
                        case "rollback":
                            if (split.length != 1) {
                                logger.warn("command '%s' expects no arguments", split[0]);
                                break;
                            }

                            txn.clear();
                            logger.info("Rolled back changes.");
                            break;
                        case "stop":
                            if (split.length != 1) {
                                logger.warn("command '%s' expects no arguments", split[0]);
                                break;
                            } else if (txn.isDirty()) {
                                logger.warn("there are uncommitted changes which must be committed or rolled back before stopping!");
                                break;
                            }

                            logger.info("stopping...");
                            break LOOP;
                        default:
                            logger.error("Unknown command: '%s'", command);
                    }
                }
            }
            txn.clear();
        }
    }
}
