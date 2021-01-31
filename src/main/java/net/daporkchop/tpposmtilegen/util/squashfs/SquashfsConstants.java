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
import lombok.experimental.UtilityClass;
import net.daporkchop.tpposmtilegen.util.squashfs.compression.Compression;

import java.io.IOException;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class SquashfsConstants {
    public final int SQUASHFS_MAGIC = 0x73717368;
    public final char SQUASHFS_VERSION_MAJOR = 4;
    public final char SQUASHFS_VERSION_MINOR = 0;

    public final int SUPERBLOCK_BYTES = 96;

    public final int COMPRESSION_ID_GZIP = 1;
    public final int COMPRESSION_ID_LZMA = 2;
    public final int COMPRESSION_ID_LZO = 3;
    public final int COMPRESSION_ID_XZ = 4;
    public final int COMPRESSION_ID_LZ4 = 5;
    public final int COMPRESSION_ID_ZSTD = 6;

    public final int UNCOMPRESSED_INODES = 0x0001;
    public final int UNCOMPRESSED_DATA = 0x0002;
    public final int CHECK = 0x0004;
    public final int UNCOMPRESSED_FRAGMENTS = 0x0008;
    public final int NO_FRAGMENTS = 0x0010;
    public final int ALWAYS_FRAGMENTS = 0x0020;
    public final int DUPLICATES = 0x0040;
    public final int EXPORTABLE = 0x0080;
    public final int UNCOMPRESSED_XATTRS = 0x0100;
    public final int NO_XATTRS = 0x0200;
    public final int COMPRESSOR_OPTIONS = 0x0400;
    public final int UNCOMPRESSED_IDS = 0x0800;

    public final int INODE_BASIC_DIRECTORY = 1;
    public final int INODE_BASIC_FILE = 2;

    public final char METABLOCK_HEADER_UNCOMPRESSED_FLAG = 0x8000;
    public final char METABLOCK_HEADER_DATA_SIZE_MASK = 0x7FFF;
    public final char METABLOCK_MAX_SIZE = 0x2000;

    public final int DIRECTORY_ENTRY_MAX_SIZE = 256;

    public final int DATA_BLOCK_UNCOMPRESSED_FLAG = 1 << 24;

    public long buildInodeReference(int metablockStart, int offset) {
        return ((long) metablockStart << 16L) | offset;
    }

    public void writeCompressedMetablock(@NonNull Compression compression, @NonNull ByteBuf src, @NonNull ByteBuf dst) throws IOException {
        int size = src.readableBytes();
        checkArg(size <= METABLOCK_MAX_SIZE, "metadata block too large: %d (must be at most %d)", size, METABLOCK_MAX_SIZE);

        if (compression.uncompressed()) {
            writeUncompressedMetablock(src, dst);
        } else {
            int startIndex = src.readerIndex();
            int headerIndex = dst.writerIndex();

            dst.writeShortLE(-1);
            compression.compress(src, dst);
            int writerIndex = dst.writerIndex();
            int compressedSize = writerIndex - headerIndex - 2;

            if (compressedSize >= size) { //block grew during compression, write it uncompressed
                src.readerIndex(startIndex);
                dst.writerIndex(headerIndex);
                writeUncompressedMetablock(src, dst);
            } else {
                dst.setShortLE(headerIndex, compressedSize);
            }
        }
    }

    private void writeUncompressedMetablock(ByteBuf src, ByteBuf dst) throws IOException {
        dst.writeShortLE(src.readableBytes() | METABLOCK_HEADER_UNCOMPRESSED_FLAG).writeBytes(src);
    }
}
