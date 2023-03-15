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
import net.daporkchop.lib.common.function.exception.ERunnable;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.natives.Memory;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Updater;
import net.daporkchop.tpposmtilegen.osm.changeset.ChangesetState;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.Utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class Update implements IMode {
    protected Storage storage;
    protected DBAccess txn;
    protected Updater updater;

    protected Serve.Server server;

    protected CompletableFuture<?> updateTask;
    protected volatile boolean updateCancelled;

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
    public synchronized void run(@NonNull String... args) throws Exception {
        Utils.setAllowForkJoinPool();

        checkArg(args.length == 1 || args.length == 2, "Usage: update <index_dir> [lite=true]");
        Path src = PFiles.assertDirectoryExists(Paths.get(args[0]));
        boolean lite = args.length == 1 || Boolean.parseBoolean(args[1]);

        Memory.releaseMemoryToSystem();

        try (Storage storage = new Storage(src, lite ? DatabaseConfig.RW_LITE : DatabaseConfig.RW_GENERAL);
             DBAccess txn = storage.db().newTransaction()) {
            this.storage = storage;
            this.txn = txn;
            this.updater = new Updater(storage);

            try (Scanner scanner = new Scanner(System.in)) {
                while (this.runCommand(scanner.nextLine())) {
                }
            }
            txn.clear();
            if (this.server != null) {
                this.server.close();
                this.server = null;
            }
        } finally {
            this.storage = null;
            this.txn = null;
            this.updater = null;
        }
    }

    private boolean runCommand(String command) throws Exception {
        String[] split = command.split(" ");

        if (this.updateTask != null) {
            if (this.updateTask.isDone()) {
                this.updateTask.join(); //this will rethrow any exceptions which may have been thrown
                this.updateTask = null;
            } else { //the update isn't done
                switch (split[0]) {
                    case "cancel":
                        logger.info("waiting for the update to be cancelled...");
                        this.updateCancelled = true;
                        this.updateTask.join();
                        this.updateTask = null;
                        break;
                    default:
                        logger.warn("an update is currently ongoing; command '%s' isn't supported at this time! only available command is 'cancel'.");
                        break;
                }
                return true;
            }
        }

        switch (split[0]) {
            case "h":
            case "help":
            case "?":
                logger.info("Available commands:\n"
                            + "  'info'\n"
                            + "  'update'\n"
                            + "  'update_to ( <target_sequence_number> | latest ) [auto_commit]'\n"
                            + "  'commit'\n"
                            + "  'rollback_to ( <target_sequence_number> | prev )'\n"
                            + "  'rollback_all'\n"
                            + "  'get ( node | way | relation | coastline ) <id>'\n"
                            + "  'serve <port>'\n"
                            + "  'stop'");
                break;
            case "info": {
                if (split.length != 1) {
                    logger.warn("command '%s' expects no arguments", split[0]);
                    break;
                }

                long databaseSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.storage.db().read()).getAsLong();
                long txnSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong();
                ChangesetState databaseState = this.storage.getChangesetState(databaseSequenceNumber, null).join();
                ChangesetState txnState = this.storage.getChangesetState(txnSequenceNumber, null).join();
                //ChangesetState latestState = storage.getLatestChangesetState().join();
                ChangesetState latestState = this.updater.globalState();

                logger.info("Database state: %s", databaseState);
                logger.info("Current state:  %s (%s)", txnState, databaseSequenceNumber == txnSequenceNumber ? "no changes" : "uncommitted");
                logger.info("Latest state:   %s", latestState);
                break;
            }
            case "update": {
                if (split.length != 1) {
                    logger.warn("command '%s' expects no arguments", split[0]);
                    break;
                }

                long originalSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong();
                long originalDataSize = this.txn.getDataSize();
                long toSequenceNumber = originalSequenceNumber + 1L;

                try {
                    logger.info("update result: %b", this.updater.update(this.storage, this.txn));
                } catch (RuntimeException e) {
                    long currentSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong();
                    checkState(originalSequenceNumber == currentSequenceNumber, "failed update still changed the sequence number?!? originally %s, currently %d", originalSequenceNumber, currentSequenceNumber);

                    long currentDataSize = this.txn.getDataSize();
                    checkState(originalDataSize == currentDataSize, "failed update still changed the data size?!? originally %s, currently %d", originalDataSize, currentDataSize);

                    ChangesetState fromState = this.storage.getChangesetState(originalSequenceNumber, null).join();
                    ChangesetState toState = this.storage.getChangesetState(toSequenceNumber, null).join();
                    logger.alert("failed to update from '%s' to '%s'!", e, fromState, toState);
                }
                break;
            }
            case "update_to": {
                if (split.length != 2 && split.length != 3) {
                    logger.warn("command '%s' expects one or two arguments: <target_sequence_number> [auto_commit]", split[0]);
                    break;
                }

                long _targetSequenceNumber;
                try {
                    _targetSequenceNumber = Long.parseUnsignedLong(split[1]);
                } catch (NumberFormatException e) {
                    if ("latest".equals(split[1])) {
                        _targetSequenceNumber = this.updater.globalState().sequenceNumber();
                    } else {
                        logger.warn("unparseable sequence number, expected integer or 'latest': %s", split[1]);
                        break;
                    }
                }
                long targetSequenceNumber = _targetSequenceNumber; //damn you java

                boolean autoCommit;
                if (split.length == 2) {
                    autoCommit = false;
                } else {
                    switch (split[2]) {
                        case "false":
                            autoCommit = false;
                            break;
                        case "true":
                            autoCommit = true;
                            break;
                        default:
                            logger.warn("unparseable value for [auto_commit], expected true/false but got: '%s'", split[2]);
                            return true;
                    }
                }

                if (this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong() >= targetSequenceNumber) {
                    logger.warn("already at sequence number %d, cannot go back in time to reach sequence number %d!",
                            this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong(), targetSequenceNumber);
                    break;
                }

                this.updateCancelled = false;
                this.updateTask = CompletableFuture.runAsync((ERunnable) () -> {
                    do {
                        long originalSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong();
                        long originalDataSize = this.txn.getDataSize();
                        long toSequenceNumber = originalSequenceNumber + 1L;

                        if (this.updateCancelled) {
                            logger.info("update_to %d: cancelled by user! stoppping at %d", targetSequenceNumber, originalSequenceNumber);
                            return;
                        }

                        try {
                            if (!this.updater.update(this.storage, this.txn)) {
                                logger.warn("updater returned false, sequence number %d isn't available yet! stopping at %d",
                                        targetSequenceNumber, this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong());
                                break;
                            }
                        } catch (RuntimeException e) {
                            long currentSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong();
                            checkState(originalSequenceNumber == currentSequenceNumber, "failed update still changed the sequence number?!? originally %s, currently %d", originalSequenceNumber, currentSequenceNumber);

                            long currentDataSize = this.txn.getDataSize();
                            checkState(originalDataSize == currentDataSize, "failed update still changed the data size?!? originally %s, currently %d", originalDataSize, currentDataSize);

                            ChangesetState fromState = this.storage.getChangesetState(originalSequenceNumber, null).join();
                            ChangesetState toState = this.storage.getChangesetState(toSequenceNumber, null).join();
                            logger.alert("failed to update from '%s' to '%s'! stopping at %d", e, fromState, toState, currentSequenceNumber);
                            break;
                        }

                        if (autoCommit) {
                            this.txn.flush();
                            Memory.releaseMemoryToSystem();
                        }
                    } while (this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong() < targetSequenceNumber);
                    logger.info("update_to %d: done.", targetSequenceNumber);
                });
                break;
            }
            case "commit":
                if (split.length != 1) {
                    logger.warn("command '%s' expects no arguments", split[0]);
                    break;
                }

                logger.info("Committing...");
                this.txn.flush();
                logger.success("Committed.");
                Memory.releaseMemoryToSystem();
                break;
            case "rollback_to": {
                if (split.length != 2) {
                    logger.warn("command '%s' expects one argument: <target_sequence_number>", split[0]);
                    break;
                }

                long targetSequenceNumber;
                try {
                    targetSequenceNumber = Long.parseUnsignedLong(split[1]);
                } catch (NumberFormatException e) {
                    if ("prev".equals(split[1])) {
                        targetSequenceNumber = this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong() - 1;
                    } else {
                        logger.warn("unparseable sequence number, expected integer or 'prev': %s", split[1]);
                        break;
                    }
                }

                if (this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong() <= targetSequenceNumber) {
                    logger.warn("already at sequence number %d, cannot go forward in time to reach sequence number %d!",
                            this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong(), targetSequenceNumber);
                    break;
                } else if (this.storage.sequenceNumberProperty().getLong(this.storage.db().read()).getAsLong() >= targetSequenceNumber) {
                    logger.warn("changes up to and including sequence number %d are already committed, cannot un-commit transactions to reach sequence number %d!",
                            this.storage.sequenceNumberProperty().getLong(this.storage.db().read()).getAsLong(), targetSequenceNumber);
                    break;
                }

                do {
                    ((DBWriteAccess.Transactional) this.txn).popCheckpoint();
                } while (this.storage.sequenceNumberProperty().getLong(this.txn).getAsLong() > targetSequenceNumber);
                logger.info("rollback_to %d: done.", targetSequenceNumber);
                break;
            }
            case "rollback_all":
                if (split.length != 1) {
                    logger.warn("command '%s' expects no arguments", split[0]);
                    break;
                } else if (!this.txn.isDirty()) {
                    logger.warn("there are no uncommitted changes to roll back!");
                    break;
                }

                this.txn.clear();
                logger.info("rolled back all uncommitted changes.");
                break;
            case "get": {
                if (split.length != 3) {
                    logger.warn("command '%s' expects 2 arguments: <type> <id>", split[0]);
                    break;
                }

                int type;
                try {
                    type = Element.typeId(split[1]);
                } catch (IllegalArgumentException e) {
                    logger.warn("invalid element type, expected one of [%s]: %s", Element.typeNames(), split[1]);
                    break;
                }

                long id;
                try {
                    id = Long.parseUnsignedLong(split[2]);
                } catch (NumberFormatException e) {
                    logger.warn("unparseable id: %s", split[2]);
                    break;
                }

                Element element = this.storage.getElement(this.txn, Element.addTypeToId(type, id));
                logger.info("%s id %d:\n  %s", Element.typeName(type), id, element);

                if (type == Node.TYPE) {
                    logger.info("  point: %s", this.storage.points().get(this.txn, id));
                }
                break;
            }
            case "serve": {
                if (split.length != 3) {
                    logger.warn("command '%s' expects 1 argument: <port>", split[0]);
                    break;
                } else if (this.server != null) {
                    logger.error("already serving!");
                    break;
                }

                int port;
                try {
                    port = Integer.parseUnsignedInt(split[1]);
                } catch (IllegalArgumentException e) {
                    logger.warn("unparseable port number: '%s'", split[1]);
                    break;
                }

                this.server = new Serve.Server(port, this.storage, this.storage.db().read());
                break;
            }
            case "stop":
                if (split.length != 1) {
                    logger.warn("command '%s' expects no arguments", split[0]);
                    break;
                } else if (this.txn.isDirty()) {
                    logger.warn("there are uncommitted changes which must be committed or rolled back before stopping!");
                    break;
                }

                logger.info("stopping...");
                return false;
            default:
                logger.error("Unknown command: '%s'", command);
        }

        return true;
    }
}
