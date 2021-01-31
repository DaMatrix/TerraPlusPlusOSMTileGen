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
import lombok.experimental.SuperBuilder;

import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
@Getter
@SuperBuilder
public abstract class Inode {
    protected final int inodeBlockStart; //the first byte of the compressed inode block
    protected final int inodeBlockOffset;

    @Builder.Default
    protected final char permissions = (char) 0644;
    protected final char uid_idx;
    protected final char gid_idx;
    protected final int modified_time;
    protected final int inode_number;

    public abstract int type();

    public abstract int basicType();

    public long reference() {
        return buildInodeReference(this.inodeBlockStart, this.inodeBlockOffset);
    }

    /**
     * Writes this inode to the given {@link ByteBuf}.
     *
     * @param dst the {@link ByteBuf}
     */
    public void write(@NonNull ByteBuf dst) {
        dst.writeShortLE(this.type())
                .writeShortLE(this.permissions)
                .writeShortLE(this.uid_idx)
                .writeShortLE(this.gid_idx)
                .writeIntLE(this.modified_time)
                .writeIntLE(this.inode_number);
    }
}
