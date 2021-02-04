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
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.osm.Updater;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;

import java.io.File;
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
        checkArg(args.length == 1, "Usage: update <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        try (Storage storage = new Storage(src.toPath());
             DBAccess txn = storage.db().newTransaction();
             Serve.Server server = new Serve.Server(8080, storage, txn)) {
            Updater updater = new Updater(storage);
            try (Scanner scanner = new Scanner(System.in)) {
                LOOP:
                while (true) {
                    String command = scanner.nextLine();
                    switch (command) {
                        case "rollback":
                            txn.clear();
                            logger.info("Rolled back changes.");
                            break;
                        case "update":
                            logger.info("update result: %b", updater.update(storage, txn));
                            break;
                        case "stop":
                            break LOOP;
                        default:
                            logger.error("Unknown command: %s", command);
                    }
                }
            }
            txn.clear();
        }
    }
}
