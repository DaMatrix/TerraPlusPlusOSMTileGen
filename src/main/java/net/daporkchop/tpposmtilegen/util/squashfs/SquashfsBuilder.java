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
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * Helper class for constructing a squashfs in a (more or less) streaming fashion.
 * <p>
 * Not thread-safe.
 *
 * @author DaPorkchop_
 */
public final class SquashfsBuilder implements AutoCloseable {
    protected final Path root;

    protected final Compression compression;

    protected final IdTableBuilder idTable;
    protected final InodeTableBuilder inodeTable;
    protected final DirectoryTableBuilder directoryTable;
    protected final BlockTableBuilder blockTable;

    protected final int blockLog;

    public SquashfsBuilder(@NonNull Compression compression, @NonNull Path workingDirectory, int blockLog) throws IOException {
        checkArg(!Files.exists(workingDirectory), "working directory already exists: %s", workingDirectory);
        this.root = Files.createDirectories(workingDirectory);
        this.compression = compression;

        this.idTable = new IdTableBuilder(compression, this.root.resolve("id"), this);
        this.inodeTable = new InodeTableBuilder(compression, this.root.resolve("inode"), this);
        this.directoryTable = new DirectoryTableBuilder(compression, this.root.resolve("directory"), this);
        this.blockTable = new BlockTableBuilder(compression, this.root.resolve("block"), this, 1 << blockLog);

        this.blockLog = blockLog;

        this.idTable.putId(tochar(0));
    }

    public void putFile(@NonNull String name, @NonNull ByteBuf contents) throws IOException {
        this.directoryTable.addFile(name, contents);
    }

    public void finish(@NonNull Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            this.directoryTable.finish();
            this.inodeTable.finish();
            this.idTable.finish();

            writeFully(channel, Unpooled.wrappedBuffer(new byte[SUPERBLOCK_BYTES]));
            ByteBuf superblock = Unpooled.buffer(SUPERBLOCK_BYTES, SUPERBLOCK_BYTES)
                    .writeIntLE(SQUASHFS_MAGIC)
                    .writeIntLE(this.inodeTable.inodes - 1)
                    .writeIntLE(toInt(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())))
                    .writeIntLE(1 << this.blockLog)
                    .writeIntLE(-1) //fragment_entry_count
                    .writeShortLE(1) //compression_id
                    .writeShortLE(this.blockLog)
                    .writeShortLE(NO_FRAGMENTS | NO_XATTRS
                                  | (this.compression.uncompressed() ? UNCOMPRESSED_DATA | UNCOMPRESSED_IDS | UNCOMPRESSED_FRAGMENTS | UNCOMPRESSED_INODES | UNCOMPRESSED_XATTRS : 0)) //flags
                    .writeShortLE(tochar(this.idTable.count())) //id_count
                    .writeShortLE(SQUASHFS_VERSION_MAJOR)
                    .writeShortLE(SQUASHFS_VERSION_MINOR)
                    .writeLongLE(this.directoryTable.root.reference());
            int bytesUsedIndex = superblock.writerIndex();
            superblock.writeLongLE(-1L); //bytes_used
            //padTo4k(channel);

            this.blockTable.transferTo(channel);
            //padTo4k(channel);

            superblock.writeLongLE(channel.position());
            this.idTable.transferTo(channel);
            //padTo4k(channel);

            superblock.writeLongLE(-1L); //xattr table?

            superblock.writeLongLE(channel.position());
            this.inodeTable.transferTo(channel);
            //padTo4k(channel);

            superblock.writeLongLE(channel.position());
            this.directoryTable.transferTo(channel);
            //padTo4k(channel);

            superblock.writeLongLE(-1L); //fragment table?

            superblock.writeLongLE(-1L); //export table?

            superblock.setLongLE(bytesUsedIndex, channel.position());
            padTo4k(channel);

            channel.position(0L);
            checkState(superblock.readableBytes() == SUPERBLOCK_BYTES);
            writeFully(channel, superblock);
        }
    }

    @Override
    public void close() throws IOException {
        this.idTable.close();
        this.inodeTable.close();
        this.directoryTable.close();
        this.blockTable.close();

        Files.delete(this.root);
    }
}
