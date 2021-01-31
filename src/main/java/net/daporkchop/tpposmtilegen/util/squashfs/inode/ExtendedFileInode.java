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
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
@SuperBuilder
@Getter
@ToString(callSuper = true)
public final class ExtendedFileInode extends Inode {
    protected final long blocks_start;
    protected final long file_size;
    protected final long sparse;
    @Builder.Default
    protected final int hard_link_count = 1;
    @Builder.Default
    protected final int fragment_block_index = -1;
    protected final int block_offset;
    @Builder.Default
    protected final int xattr_idx = -1;
    @NonNull
    protected final int[] block_sizes;

    @Override
    public int type() {
        return INODE_EXTENDED_FILE;
    }

    @Override
    public int basicType() {
        return INODE_BASIC_FILE;
    }

    @Override
    public void write(@NonNull ByteBuf dst) {
        super.write(dst);

        dst.writeLongLE(this.blocks_start)
                .writeLongLE(this.file_size)
                .writeLongLE(this.sparse)
                .writeIntLE(this.hard_link_count)
                .writeIntLE(this.fragment_block_index)
                .writeIntLE(this.block_offset)
                .writeIntLE(this.xattr_idx);
        for (int block_size : this.block_sizes) {
            dst.writeIntLE(block_size);
        }
    }
}
