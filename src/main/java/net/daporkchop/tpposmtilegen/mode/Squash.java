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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOBiConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.SquashfsBuilder;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.ZstdCompression;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.TreeMap;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;

/**
 * @author DaPorkchop_
 */
public class Squash implements IMode {
    @Override
    public String name() {
        return "squash";
    }

    @Override
    public String synopsis() {
        return "<src_dir> <squashfs>";
    }

    @Override
    public String help() {
        return "Packs a given directory into a squashfs.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: squash <src_dir> <squashfs>");
        Path dst = Paths.get(args[1]);

        try (SquashfsBuilder builder = new SquashfsBuilder(new ZstdCompression(), dst.resolveSibling(dst.getFileName().toString() + ".tmp"), dst, 19);
             ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Squash")
                     .slot("files").slot("directories").build()) {
            this.squashRecursive(builder, notifier, null, Paths.get(args[0]));
        }
    }

    private void squashRecursive(@NonNull SquashfsBuilder builder, @NonNull ProgressNotifier notifier, Path fakeRoot, @NonNull Path dir) throws IOException {
        Map<String, BasicFileAttributes> namesToAttrs = new TreeMap<>();
        Files.find(dir, 1, (path, attrs) -> {
            if (!dir.equals(path)) {
                namesToAttrs.put(path.getFileName().toString(), attrs);
            }
            return false;
        }).forEach(p -> {
            throw new UnsupportedOperationException();
        });

        namesToAttrs.forEach((IOBiConsumer<String, BasicFileAttributes>) (name, attrs) -> {
            Path filePath = dir.resolve(name);
            Path outputPath = fakeRoot != null ? fakeRoot.resolve(name) : Paths.get(name);
            if (attrs.isDirectory()) {
                this.squashRecursive(builder, notifier, outputPath, filePath);
                notifier.step(1);
            } else {
                SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
                ByteBuf buf = recycler.get();
                try {
                    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                        readFully(channel, buf, toInt(attrs.size(), filePath));
                    }
                    builder.putFile(outputPath, buf);
                } finally {
                    recycler.release(buf);
                }
                notifier.step(0);
            }
        });
    }
}
