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
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.util.squashfs.SquashfsBuilder;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.NoCompression;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.ZstdCompression;

import java.io.File;
import java.nio.charset.StandardCharsets;
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
        return "<index_dir> <squashfs>";
    }

    @Override
    public String help() {
        return "Exports all changed files.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: export <index_dir> <squashfs>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        Path dst = Paths.get(args[1]);
        Files.deleteIfExists(dst);

        try (SquashfsBuilder builder = new SquashfsBuilder(NoCompression.INSTANCE, dst.resolveSibling(dst.getFileName().toString() + ".tmp"), 19)) {
            builder.putFile("asdf.txt", Unpooled.wrappedBuffer("12345".getBytes(StandardCharsets.UTF_8)));

            builder.finish(dst);
        }

        /*try (Storage storage = new Storage(src.toPath())) {
            throw new UnsupportedOperationException();
        }*/
    }
}
