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
import java.nio.charset.StandardCharsets;
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
    protected final DirectoryTableBuilder directoryTable;
    protected final DatablockBuilder blockTable;
    protected final FragmentTableBuilder fragmentTable;

    protected final int blockLog;

    public SquashfsBuilder(@NonNull Compression compression, @NonNull Path workingDirectory, int blockLog) throws IOException {
        checkArg(!Files.exists(workingDirectory), "working directory already exists: %s", workingDirectory);
        this.root = Files.createDirectories(workingDirectory);
        this.compression = compression;

        this.blockLog = blockLog;

        this.idTable = new IdTableBuilder(compression, this.root.resolve("id"), this);
        this.directoryTable = new DirectoryTableBuilder(compression, this.root.resolve("directory"), this);
        this.blockTable = new DatablockBuilder(compression, this.root.resolve("block"), this);
        this.fragmentTable = new FragmentTableBuilder(compression, this.root.resolve("fragment"), this);

        this.idTable.putId(tochar(1000));
    }

    public void putFile(@NonNull String name, @NonNull ByteBuf contents) throws IOException {
        byte[] arr = new byte[(1 << this.blockLog) + 1];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (byte) i;
        }
        for (int i = 0; i < 123; i++) {
            this.directoryTable.startDirectory(String.format("%03d", i));
            this.directoryTable.addFile("asdf", Unpooled.copiedBuffer("hello world!", StandardCharsets.UTF_8));
            this.directoryTable.addFile("jklo", Unpooled.wrappedBuffer(arr));
            this.directoryTable.endDirectory();
        }

        this.directoryTable.addFile(name, contents);
    }

    public void finish(@NonNull Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            writeFully(channel, Unpooled.wrappedBuffer(new byte[SUPERBLOCK_BYTES])); //write blank superblock

            Superblock superblock = new Superblock()
                    .modification_time(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
                    .block_log(this.blockLog)
                    .block_size(1L << this.blockLog)
                    .flags(NO_XATTRS)
                    .compression_id(this.compression.id());

            this.fragmentTable.finish(channel, superblock);
            this.blockTable.finish(channel, superblock);
            this.directoryTable.finish(channel, superblock);
            this.idTable.finish(channel, superblock);

            this.blockTable.transferTo(channel, superblock);
            superblock.xattr_id_table_start(0xFFFFFFFFL); //xattr table?
            this.directoryTable.transferTo(channel,superblock);
            this.fragmentTable.transferTo(channel, superblock);
            superblock.export_table_start(0xFFFFFFFFL); //export table?
            this.idTable.transferTo(channel, superblock); //apparently the id table needs to be at the end

            superblock.bytes_used(channel.position());

            channel.position(0L);

            SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
            ByteBuf buf = recycler.get();
            try {
                superblock.write(buf);
                checkState(buf.readableBytes() == SUPERBLOCK_BYTES);
                writeFully(channel, buf);
            } finally {
                recycler.release(buf);
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.idTable.close();
        this.directoryTable.close();
        this.blockTable.close();
        this.fragmentTable.close();

        Files.delete(this.root);
    }
}
