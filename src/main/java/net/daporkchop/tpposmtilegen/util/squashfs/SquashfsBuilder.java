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
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.*;
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

    protected final FileChannel channel;

    protected final IdTableBuilder idTable;
    protected final DirectoryTableBuilder directoryTable;
    protected final DatablockBuilder blockTable;
    protected final FragmentTableBuilder fragmentTable;

    protected final int blockLog;

    protected Path lastPath;

    public SquashfsBuilder(@NonNull Compression compression, @NonNull Path workingDirectory, @NonNull Path dst, int blockLog) throws IOException {
        checkArg(!Files.exists(workingDirectory), "working directory already exists: %s", workingDirectory);

        this.channel = FileChannel.open(dst, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        writeFully(this.channel, Unpooled.wrappedBuffer(new byte[SUPERBLOCK_BYTES])); //write blank superblock

        this.root = Files.createDirectories(workingDirectory);
        this.compression = compression;

        this.blockLog = blockLog;

        this.idTable = new IdTableBuilder(compression, this.root.resolve("id"), this);
        this.directoryTable = new DirectoryTableBuilder(compression, this.root.resolve("directory"), this);
        this.blockTable = new DatablockBuilder(this.root.resolve("block"), this);
        this.fragmentTable = new FragmentTableBuilder(compression, this.root.resolve("fragment"), this);

        this.idTable.putId(tochar(1000));
    }

    public void putFile(@NonNull Path path, @NonNull ByteBuf contents) throws IOException {
        int newCount = path.getNameCount();

        int highestCommonIndex = -1;
        if (this.lastPath != null) {
            int oldCount = this.lastPath.getNameCount();
            for (int i = 0, lim = min(newCount, oldCount) - 1; i < lim; i++) {
                if (!path.getName(i).equals(this.lastPath.getName(i))) {
                    for (; i < oldCount - 1; i++) {
                        this.directoryTable.endDirectory();
                    }
                    break;
                } else {
                    highestCommonIndex = i;
                }
            }
        }
        for (int i = highestCommonIndex + 1; i < newCount - 1; i++) {
            this.directoryTable.startDirectory(path.getName(i).toString());
        }

        this.directoryTable.addFile(path.getFileName().toString(), contents);
        this.lastPath = path;
    }

    @Override
    public void close() throws IOException {
        try {
            Superblock superblock = new Superblock()
                    .modification_time(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
                    .block_log(this.blockLog)
                    .block_size(1L << this.blockLog)
                    .flags(NO_XATTRS)
                    .compression_id(this.compression.id());

            this.fragmentTable.finish(this.channel, superblock);
            this.blockTable.finish(this.channel, superblock);
            this.directoryTable.finish(this.channel, superblock);
            this.idTable.finish(this.channel, superblock);

            superblock.xattr_id_table_start(0xFFFFFFFFL); //xattr table?
            this.directoryTable.transferTo(this.channel, superblock);
            this.fragmentTable.transferTo(this.channel, superblock);
            superblock.export_table_start(0xFFFFFFFFL); //export table?
            this.idTable.transferTo(this.channel, superblock); //apparently the id table needs to be at the end

            superblock.bytes_used(this.channel.position());
            this.channel.position(0L);

            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf buf = recycler.get();
            try {
                superblock.write(buf);
                checkState(buf.readableBytes() == SUPERBLOCK_BYTES);
                writeFully(this.channel, buf);
            } finally {
                recycler.release(buf);
            }
        } finally {
            this.idTable.close();
            this.directoryTable.close();
            this.blockTable.close();
            this.fragmentTable.close();

            Files.delete(this.root);
        }
    }
}
