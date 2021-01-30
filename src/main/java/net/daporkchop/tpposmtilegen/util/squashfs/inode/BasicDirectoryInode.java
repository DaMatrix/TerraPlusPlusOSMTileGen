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

package net.daporkchop.tpposmtilegen.util.squashfs.inode;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
@SuperBuilder
public final class BasicDirectoryInode extends Inode {
    protected final int block_idx;
    protected final int hard_link_count;
    protected final char file_size;
    protected final char block_offset;
    protected final int parent_inode_number;

    @Override
    public int type() {
        return INODE_BASIC_DIRECTORY;
    }

    @Override
    public int basicType() {
        return INODE_BASIC_DIRECTORY;
    }

    @Override
    public void write(@NonNull ByteBuf dst) {
        super.write(dst);

        dst.writeIntLE(this.block_idx)
                .writeIntLE(this.hard_link_count)
                .writeShortLE(this.file_size)
                .writeShortLE(this.block_offset)
                .writeIntLE(this.parent_inode_number);
    }
}