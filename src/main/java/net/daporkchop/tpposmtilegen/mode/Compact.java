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
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.util.CloseableThreadFactory;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class Compact implements IMode {
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
        Path src = PFiles.assertDirectoryExists(Paths.get(args[0]));

        try (Storage storage = new Storage(src, DatabaseConfig.RW_BULK_LOAD);
             CloseableThreadFactory threadFactory = new CloseableThreadFactory("Compaction worker")) {
            logger.info("Running compaction...");

            for (ColumnFamilyHandle column : storage.db().columns().keySet()) {
                try (TimedOperation operation = new TimedOperation("Compact " + new String(column.getName(), StandardCharsets.UTF_8))) {
                    storage.db().delegate().compactRange(column, null, null, storage.db().config().compactRangeOptions());
                }
            }
        }
        logger.success("Done.");
    }
}
