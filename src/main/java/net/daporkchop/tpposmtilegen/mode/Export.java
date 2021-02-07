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

import io.netty.buffer.Unpooled;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOBiConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.squashfs.SquashfsBuilder;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.NoCompression;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.ZstdCompression;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class Export implements IMode {
    @Override
    public String name() {
        return "export";
    }

    @Override
    public String synopsis() {
        return "<index_dir> <squashfs> <compression>";
    }

    @Override
    public String help() {
        return "Exports all changed files.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 3, "Usage: export <index_dir> <squashfs> <compression>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        Path dst = Paths.get(args[1]);

        try (Storage storage = new Storage(src.toPath(), Database.DB_OPTIONS_LITE, true)) {
            try (SquashfsBuilder builder = new SquashfsBuilder(Squash.compressionForName(args[2]), dst.resolveSibling(dst.getFileName().toString() + ".tmp"), dst, 19);
                 ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Export files")
                         .slot("files").build()) {
                storage.files().forEach(storage.db().read(),
                        (IOBiConsumer<String, ByteBuffer>) (name, data) -> {
                            builder.putFile(Paths.get(name), Unpooled.wrappedBuffer(data));
                            notifier.step(0);
                        });
            }
        }
    }
}
