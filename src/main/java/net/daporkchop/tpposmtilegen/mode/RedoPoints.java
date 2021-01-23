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

import com.wolt.osm.parallelpbf.ParallelBinaryParser;
import com.wolt.osm.parallelpbf.entity.Header;
import lombok.NonNull;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.CloseableThreadFactory;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class RedoPoints implements IMode {
    @Override
    public String name() {
        return "redo_points";
    }

    @Override
    public String synopsis() {
        return "<planet-latest.osm.pbf> <index_dir>";
    }

    @Override
    public String help() {
        return "Removes and re-adds all points.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: redo_points <pbf> <index_dir>");
        File src = PFiles.assertFileExists(new File(args[0]));
        File dst = new File(args[1]);

        try (Storage storage = new Storage(dst.toPath());
             InputStream is = new FileInputStream(src);
             CloseableThreadFactory threadFactory = new CloseableThreadFactory("PBF parse worker")) {

            logger.info("Clearing points DB...");
            storage.points().clear();
            logger.success("Done.");

            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Read PBF")
                    .slot("nodes")
                    .build()) {
                new ParallelBinaryParser(is, PorkUtil.CPU_COUNT)
                        .setThreadFactory(threadFactory)
                        .onHeader((EConsumer<Header>) header -> logger.info("PBF header: %s", header))
                        .onBoundBox(bb -> logger.info("bounding box: %s", bb))
                        .onChangeset(changeset -> logger.info("changeset: %s", changeset))
                        .onNode((EConsumer<com.wolt.osm.parallelpbf.entity.Node>) in -> {
                            storage.points().put(storage.db().batch(), in.getId(), new Point(in.getLon(), in.getLat()));

                            notifier.step(0);
                        })
                        .parse();
            }

            //ensure everything is written to disk before advancing
            storage.flush();
        }
    }
}
