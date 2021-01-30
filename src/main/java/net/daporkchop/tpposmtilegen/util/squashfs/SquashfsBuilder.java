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

package net.daporkchop.tpposmtilegen.util.squashfs;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * Helper class for constructing a squashfs in a (more or less) streaming fashion.
 * <p>
 * Not thread-safe.
 *
 * @author DaPorkchop_
 */
public final class SquashfsBuilder implements AutoCloseable {
    protected final Path root;

    protected final InodeTableBuilder inodeTable;
    protected final DirectoryTableBuilder directoryTable;

    public SquashfsBuilder(@NonNull Compression compression, @NonNull Path workingDirectory) throws IOException {
        checkArg(!Files.exists(workingDirectory), "working directory already exists: %s", workingDirectory);
        this.root = Files.createDirectories(workingDirectory);

        this.inodeTable = new InodeTableBuilder(compression, this.root.resolve("inode"), this);
        this.directoryTable = new DirectoryTableBuilder(compression, this.root.resolve("directory"), this);
    }

    public void putFile(@NonNull String name, @NonNull ByteBuf contents) {
    }

    @Override
    public void close() throws IOException {
        this.inodeTable.close();

        Files.delete(this.root);
    }
}
