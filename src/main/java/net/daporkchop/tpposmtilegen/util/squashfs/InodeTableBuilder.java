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
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.Inode;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;

/**
 * @author DaPorkchop_
 */
final class InodeTableBuilder implements ISquashfsBuilder {
    protected final SquashfsBuilder parent;
    protected final MetablockSequence writer;

    protected long inodes = 0L; //the total number of inodes created so far

    public InodeTableBuilder(@NonNull Compression compression, @NonNull Path root, @NonNull SquashfsBuilder parent) throws IOException {
        this.parent = parent;
        this.writer = new MetablockSequence(compression, root, parent, "inode table");
    }

    public <C extends Inode, B extends Inode.InodeBuilder<C, ?>> C append(@NonNull B builder) throws IOException {
        C inode = builder.inode_number(u32(++this.inodes))
                .inodeBlockStart(toInt(this.writer.rawChannel.position()) + this.writer.writtenBlockCount() * 2)
                .inodeBlockOffset(this.writer.buffer.readableBytes())
                .build();

        //encode inode
        SimpleRecycler<ByteBuf> recycler = IO_BUFFER_RECYCLER.get();
        ByteBuf buf = recycler.get();
        try {
            inode.write(buf);
            this.writer.write(buf);
        } finally {
            recycler.release(buf);
        }

        return inode;
    }

    @Override
    public void finish(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        this.writer.finish(channel, superblock);

        superblock.inode_count(this.inodes);
    }

    @Override
    public void transferTo(@NonNull FileChannel channel, @NonNull Superblock superblock) throws IOException {
        superblock.inode_table_start(channel.position());

        this.writer.transferTo(channel, superblock);
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
