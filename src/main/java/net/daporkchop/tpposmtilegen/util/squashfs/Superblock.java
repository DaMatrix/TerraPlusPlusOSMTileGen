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
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.squashfs.inode.Inode;

import static net.daporkchop.tpposmtilegen.util.Utils.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString
final class Superblock {
    private long inode_count = -1L;
    private long modification_time = -1L;
    private long block_size = -1L;
    private long fragment_entry_count = -1L;
    private int compression_id = -1;
    private int block_log = -1;
    private int flags = -1;
    private int id_count = -1;
    private Inode root_inode;
    private long bytes_used = -1L;
    private long id_table_start = -1L;
    private long xattr_id_table_start = -1L;
    private long inode_table_start = -1L;
    private long directory_table_start = -1L;
    private long fragment_table_start = -1L;
    private long export_table_start = -1L;

    public void write(@NonNull ByteBuf dst) {
        dst.writeIntLE(SQUASHFS_MAGIC)
                .writeIntLE(u32(this.inode_count))
                .writeIntLE(u32(this.modification_time))
                .writeIntLE(u32(this.block_size))
                .writeIntLE(u32(this.fragment_entry_count))
                .writeShortLE(u16(this.compression_id))
                .writeShortLE(u16(this.block_log))
                .writeShortLE(u16(this.flags))
                .writeShortLE(u16(this.id_count))
                .writeShortLE(SQUASHFS_VERSION_MAJOR)
                .writeShortLE(SQUASHFS_VERSION_MINOR)
                .writeLongLE(u64(this.root_inode.reference()))
                .writeLongLE(u64(this.bytes_used))
                .writeLongLE(u64(this.id_table_start))
                .writeLongLE(-1L) //TODO: .writeLongLE(u64(this.xattr_id_table_start))
                .writeLongLE(u64(this.inode_table_start))
                .writeLongLE(u64(this.directory_table_start))
                .writeLongLE(u64(this.fragment_table_start))
                .writeLongLE(-1L)//TODO: .writeLongLE(u64(this.export_table_start))
        ;
    }
}
