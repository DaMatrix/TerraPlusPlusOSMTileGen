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
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.io.File;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class PurgeOSM implements IMode {
    @Override
    public String name() {
        return "purge_osm";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "Deletes all OSM data.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: purge <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        try (Storage storage = new Storage(src.toPath())) {
            storage.nodes().clear();
            storage.points().clear();
            storage.ways().clear();
            storage.relations().clear();
            storage.references().clear();
            storage.sequenceNumber().clear();
        }
    }
}
