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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.Inode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * @author DaPorkchop_
 */
final class DirectoryTableBuilder implements AutoCloseable {
    protected final SquashfsBuilder parent;
    protected final Path workingDir;
    protected final MetablockWriter writer;

    protected final Deque<Directory> stack = new ArrayDeque<>();

    public DirectoryTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;
        this.workingDir = Files.createDirectories(root);

        this.writer = new MetablockWriter(compression, root.resolve("metablocks"));
    }

    public void startDirectory(@NonNull String name) throws IOException {
    }

    public void endDirectory() throws IOException {
        try (Directory directory = this.stack.pop()) {
            this.stack.getFirst().appendDirectory(directory);
        }
    }

    @Override
    public void close() throws IOException {
        this.writer.close();

        Files.delete(this.workingDir);
    }

    @RequiredArgsConstructor
    protected class Directory implements AutoCloseable {
        @NonNull
        protected final String name;
        protected final List<AbsoluteDirectoryEntry> entries = new ArrayList<>();

        public void appendDirectory(@NonNull Directory directory) throws IOException {
        }

        public AbsoluteDirectoryEntry toEntry

        @Override
        public void close() throws IOException {
        }
    }

    @RequiredArgsConstructor
    @ToString
    protected static class AbsoluteDirectoryEntry {
        @NonNull
        public final Inode inode;
        @NonNull
        public final String name;
    }
}
