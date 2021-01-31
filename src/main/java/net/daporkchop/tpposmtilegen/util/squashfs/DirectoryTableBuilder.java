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
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.BasicDirectoryInode;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.BasicFileInode;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.Inode;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
final class DirectoryTableBuilder implements ISquashfsBuilder {
    protected final SquashfsBuilder parent;
    protected final Path workingDir;
    protected final MetablockWriter writer;

    protected final Deque<Directory> stack = new ArrayDeque<>();

    protected Inode root;

    public DirectoryTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;
        this.workingDir = Files.createDirectories(root);

        this.writer = new MetablockWriter(compression, root.resolve("metablocks"));

        this.stack.push(new Directory(""));
    }

    public void startDirectory(@NonNull String name) throws IOException {
        this.stack.push(new Directory(name));
    }

    public void endDirectory() throws IOException {
        Directory directory = this.stack.pop();
        this.stack.getFirst().appendDirectory(directory);
    }

    public void addFile(@NonNull String name, @NonNull ByteBuf contents) throws IOException {
        long blocksStart = this.parent.blockTable.writtenBytes + SUPERBLOCK_BYTES;

        Inode inode = this.parent.inodeTable.append(BasicFileInode.builder()
                .blocks_start(toInt(blocksStart))
                .fragment_block_index(-1)
                .block_offset(0)
                .file_size(contents.readableBytes())
                .block_sizes(this.parent.blockTable.write(contents)));
        this.stack.getFirst().entries.add(new AbsoluteDirectoryEntry(inode, name));
    }

    protected AbsoluteDirectoryEntry toEntry(@NonNull Directory directory) throws IOException {
        Inode directoryInode;

        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf dst = recycler.get();
        try {
            for (int i = 0, size = directory.entries.size(); i < size; ) {
                int startIndex = dst.writerIndex();

                AbsoluteDirectoryEntry startEntry = directory.entries.get(i);
                int inodeBlockStart = startEntry.inode.inodeBlockStart();
                int inodeNumber = startEntry.inode.inode_number();
                dst.writeIntLE(-1)
                        .writeIntLE(inodeBlockStart)
                        .writeIntLE(inodeNumber);

                int j = 0;
                for (; j < DIRECTORY_ENTRY_MAX_SIZE && i + j < size; j++) {
                    AbsoluteDirectoryEntry entry = directory.entries.get(i + j);
                    if (entry.inode.inodeBlockStart() != inodeBlockStart || inodeNumber - entry.inode.inode_number() > Short.MAX_VALUE) {
                        break;
                    }

                    dst.writeShortLE(entry.inode.inodeBlockOffset())
                            .writeShortLE(inodeNumber - entry.inode.inode_number())
                            .writeShortLE(entry.inode.basicType());

                    int nameSizeIndex = dst.writerIndex();
                    int nameSize = dst.writeShortLE(-1).writeCharSequence(entry.name, StandardCharsets.UTF_8);
                    dst.setShortLE(nameSizeIndex, nameSize - 1);
                }
                dst.setIntLE(startIndex, j - 1);
                i += j;
            }

            directoryInode = this.parent.inodeTable.append(BasicDirectoryInode.builder()
                    .block_idx(this.writer.blocksWritten)
                    .block_offset(tochar(this.writer.buffer.readableBytes()))
                    .hard_link_count(1)
                    .file_size(tochar(dst.readableBytes()))
                    .parent_inode_number(1)); //TODO: this is wrong, parent inode should only be 1 on root directory

            this.writer.write(dst);
        } finally {
            recycler.release(dst);
        }

        return new AbsoluteDirectoryEntry(directoryInode, directory.name);
    }

    @Override
    public void finish(@NonNull Superblock superblock) throws IOException {
        AbsoluteDirectoryEntry entry = this.toEntry(this.stack.pop());
        checkState(this.stack.isEmpty());

        this.writer.finish(superblock);

        superblock.root_inode(entry.inode);
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        superblock.directory_table_start(channel.position());

        this.writer.transferTo(channel, superblock);
    }

    @Override
    public void close() throws IOException {
        this.writer.close();

        Files.delete(this.workingDir);
    }

    @RequiredArgsConstructor
    @ToString
    protected static class AbsoluteDirectoryEntry {
        @NonNull
        public final Inode inode;
        @NonNull
        public final String name;
    }

    @RequiredArgsConstructor
    protected class Directory {
        @NonNull
        protected final String name;
        protected final List<AbsoluteDirectoryEntry> entries = new ArrayList<>();

        public void appendDirectory(@NonNull Directory directory) throws IOException {
            this.entries.add(DirectoryTableBuilder.this.toEntry(directory));
        }
    }
}
