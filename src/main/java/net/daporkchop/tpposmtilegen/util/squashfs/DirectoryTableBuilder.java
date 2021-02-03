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
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
final class DirectoryTableBuilder implements ISquashfsBuilder {
    static void writeString(@NonNull ByteBuf dst, @NonNull String text) {
        int lengthIndex = dst.writerIndex();
        int length = dst.writeIntLE(0).writeCharSequence(text, StandardCharsets.US_ASCII);
        dst.setIntLE(lengthIndex, length);
    }

    static String readString(@NonNull ByteBuf src) {
        return src.readCharSequence(src.readIntLE(), StandardCharsets.US_ASCII).toString();
    }

    protected final SquashfsBuilder parent;
    protected final Path root;

    protected final ImmediateMetablockSequence directoryTableWriter;
    protected final ImmediateMetablockSequence inodeTableWriter;

    protected final Path indexFile;
    protected final FileChannel indexChannel;

    protected final Deque<Dir> stack = new ArrayDeque<>();

    private int inodeIdAllocator = 1;

    public DirectoryTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;
        this.root = Files.createDirectories(root);

        this.directoryTableWriter = new ImmediateMetablockSequence(compression, root.resolve("directories"), parent);
        this.inodeTableWriter = new ImmediateMetablockSequence(compression, root.resolve("inodes"), parent);

        this.indexFile = root.resolve("index");
        this.indexChannel = FileChannel.open(this.indexFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        this.startDirectory("");
    }

    public void startDirectory(@NonNull String name) throws IOException {
        this.stack.push(new Dir(name));
    }

    public void endDirectory() throws IOException {
        try (Dir dir = this.stack.pop()) {
            dir.finish();

            long pos = this.indexChannel.position();
            writeFully(this.indexChannel, dir.buffer);

            this.stack.getFirst().appendDirectory(dir.name, pos);
        }
    }

    public void addFile(@NonNull String name, @NonNull ByteBuf contents) throws IOException {
        int fullFileSize = contents.readableBytes();

        long blockReferenceIndex = -1L;
        int blocks = contents.readableBytes() >> this.parent.blockLog;
        if (blocks > 0) {
            blockReferenceIndex = this.parent.blockTable.writeBlocks(contents.readSlice(blocks << this.parent.blockLog), blocks, false);
        }

        int fragmentIndex = -1;
        int fragmentOffset = 0;
        if (contents.isReadable()) {
            FragmentBlockEntry fragmentEntry = this.parent.fragmentTable.appendFragmentData(contents);
            fragmentIndex = fragmentEntry.fragment_block_index;
            fragmentOffset = fragmentEntry.block_offset;
        }

        this.stack.getFirst().appendFile(name, fullFileSize, fragmentIndex, fragmentOffset, blockReferenceIndex);
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        while (this.stack.size() > 1) {
            this.endDirectory();
        }

        long rootIndexPos;
        try (Dir dir = this.stack.pop()) {
            dir.finish();

            rootIndexPos = this.indexChannel.position();
            writeFully(this.indexChannel, dir.buffer);
        }

        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf buf = recycler.get();
        try {
            superblock.root_inode(this.writeDirectory(toInt(superblock.modification_time()), 1, rootIndexPos).reference())
                    .inode_count(this.inodeIdAllocator - 1);
        } finally {
            recycler.release(buf);
        }

        this.inodeTableWriter.finish(channel, superblock);
        this.directoryTableWriter.finish(channel, superblock);
    }

    private InodeReference writeDirectory(int timestamp, int parentInode, long indexPos) throws IOException {
        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf readBuffer = recycler.get();
        ByteBuf inodeWriteBuffer = recycler.get();
        ByteBuf directoryEntriesBuffer = recycler.get();
        try {
            readFully(this.indexChannel, indexPos, readBuffer, 4);
            int size = readBuffer.readIntLE();
            readFully(this.indexChannel, indexPos + 4L, readBuffer.clear(), size);

            int inodeNumber = this.inodeIdAllocator++;
            inodeWriteBuffer.writeShortLE(INODE_EXTENDED_DIRECTORY) //inode_type
                    .writeShortLE(0773) //permissions
                    .writeShortLE(0).writeShortLE(0) //uid_idx & gid_idx
                    .writeIntLE(timestamp)
                    .writeIntLE(inodeNumber)
                    .writeIntLE(1); //hard_link_count
            int file_sizeIndex = inodeWriteBuffer.writerIndex();
            inodeWriteBuffer.writeIntLE(-1);
            int block_idxIndex = inodeWriteBuffer.writerIndex();
            inodeWriteBuffer.writeIntLE(-1)
                    .writeIntLE(parentInode)
                    .writeShortLE(0); //index_count
            int block_offsetIndex = inodeWriteBuffer.writerIndex();
            inodeWriteBuffer.writeShortLE(-1)
                    .writeIntLE(-1); //xattr_idx

            int countIndex = -1;
            int lastStart = -1;
            int lastBaseInode = -1;
            int count = 0;

            while (readBuffer.isReadable()) {
                boolean isDirectory = readBuffer.readBoolean();
                String name = readString(readBuffer);

                InodeReference childReference;
                if (isDirectory) {
                    childReference = this.writeDirectory(timestamp, inodeNumber, readBuffer.readLongLE());
                } else {
                    childReference = this.writeFile(timestamp, readBuffer);
                }

                if (count == 0
                    || count++ == DIRECTORY_ENTRY_MAX_COUNT
                    || lastStart != childReference.blockStart
                    || childReference.inodeNumber - lastBaseInode > Short.MAX_VALUE) { //begin new entry
                    if (count != 0) { //finish last entry
                        directoryEntriesBuffer.setIntLE(countIndex, count - 2);
                    }

                    count = 1;
                    countIndex = directoryEntriesBuffer.writerIndex();
                    directoryEntriesBuffer.writeIntLE(-1)
                            .writeIntLE(lastStart = childReference.blockStart)
                            .writeIntLE(lastBaseInode = childReference.inodeNumber);
                }

                directoryEntriesBuffer.writeShortLE(childReference.blockOffset)
                        .writeShortLE(childReference.inodeNumber - lastBaseInode)
                        .writeShortLE(isDirectory ? INODE_BASIC_DIRECTORY : INODE_BASIC_FILE)
                        .writeShortLE(name.length() - 1)
                        .writeCharSequence(name, StandardCharsets.US_ASCII);
            }

            if (count != 0) { //finish last entry
                directoryEntriesBuffer.setIntLE(countIndex, count - 1);
            }

            inodeWriteBuffer.setIntLE(file_sizeIndex, directoryEntriesBuffer.readableBytes())
                    .setIntLE(block_idxIndex, this.directoryTableWriter.bytesWritten())
                    .setShortLE(block_offsetIndex, this.directoryTableWriter.inBlockOffset());

            InodeReference reference = new InodeReference(this.inodeTableWriter.bytesWritten(), this.inodeTableWriter.inBlockOffset(), inodeNumber);
            this.inodeTableWriter.write(inodeWriteBuffer);
            this.directoryTableWriter.write(directoryEntriesBuffer);
            return reference;
        } finally {
            recycler.release(directoryEntriesBuffer);
            recycler.release(inodeWriteBuffer);
            recycler.release(readBuffer);
        }
    }

    protected InodeReference writeFile(int timestamp, @NonNull ByteBuf readBuffer) throws IOException {
        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf writeBuffer = recycler.get();
        try {
            int inodeNumber = this.inodeIdAllocator++;
            writeBuffer.writeShortLE(INODE_EXTENDED_FILE) //inode_type
                    .writeShortLE(0773) //permissions
                    .writeShortLE(0).writeShortLE(0) //uid_idx & gid_idx
                    .writeIntLE(timestamp)
                    .writeIntLE(inodeNumber);

            long fileSize = readBuffer.readLongLE();

            int block_startIndex = writeBuffer.writerIndex();
            writeBuffer.writeLongLE(-1L)
                    .writeLongLE(fileSize)
                    .writeLongLE(0L) //sparse
                    .writeIntLE(1) //hard_link_count
                    .writeIntLE(readBuffer.readIntLE()) //fragment_block_index
                    .writeIntLE(readBuffer.readIntLE()) //block_offset
                    .writeIntLE(-1); //xattr_idx

            int blockSize = 1 << this.parent.blockLog;
            if (fileSize >= blockSize) {
                long blockReferenceIndex = readBuffer.readLongLE();
                DatablockReference blockReference = this.parent.blockTable.getReference(blockReferenceIndex);
                writeBuffer.setLongLE(block_startIndex, blockReference.blockStart());
                writeBuffer.writeIntLE(blockReference.size());
                for (int i = 1; (long) i * blockSize < (fileSize & -blockSize); i++) {
                    writeBuffer.writeIntLE(this.parent.blockTable.getReference(blockReferenceIndex + 1).size());
                }
            }

            InodeReference reference = new InodeReference(this.inodeTableWriter.bytesWritten(), this.inodeTableWriter.inBlockOffset(), inodeNumber);
            this.inodeTableWriter.write(writeBuffer);
            return reference;
        } finally {
            recycler.release(writeBuffer);
        }
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        superblock.directory_table_start(channel.position());

        superblock.inode_table_start(channel.position());
        this.inodeTableWriter.transferTo(channel, superblock);
        superblock.directory_table_start(channel.position());
        this.directoryTableWriter.transferTo(channel, superblock);
    }

    @Override
    public void close() throws IOException {
        this.indexChannel.close();
        Files.delete(this.indexFile);

        this.inodeTableWriter.close();
        this.directoryTableWriter.close();

        Files.delete(this.root);
    }

    @RequiredArgsConstructor
    static class Dir implements AutoCloseable {
        protected final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.ioBuffer().writeIntLE(-1);
        @NonNull
        protected final String name;

        protected String prevName;

        public void appendDirectory(@NonNull String name, long addr) {
            this.checkValidName(name);
            this.buffer.writeBoolean(true);
            writeString(this.buffer, name);
            this.buffer.writeLongLE(addr);
        }

        public void appendFile(@NonNull String name, long fileSize, int fragmentIndex, int fragmentOffset, long blockReferenceIndex) {
            this.checkValidName(name);
            this.buffer.writeBoolean(false);
            writeString(this.buffer, name);
            this.buffer.writeLongLE(fileSize)
                    .writeIntLE(fragmentIndex)
                    .writeIntLE(fragmentOffset);
            if (blockReferenceIndex != -1L) {
                this.buffer.writeLongLE(blockReferenceIndex);
            }
        }

        public void finish() {
            this.buffer.setIntLE(0, this.buffer.readableBytes() - 4);
        }

        private void checkValidName(@NonNull String name) {
            if (this.prevName != null) {
                checkArg(this.prevName.compareTo(name) < 0, "new name must be greater than old name: \"%s\" -> \"%s\"", this.prevName, name);
            }
            this.prevName = name;
        }

        @Override
        public void close() {
            this.buffer.release();
        }
    }

    @RequiredArgsConstructor
    static class InodeReference {
        final int blockStart;
        final int blockOffset;
        final int inodeNumber;

        protected long reference() {
            return buildInodeReference(this.blockStart, this.blockOffset);
        }
    }
}
