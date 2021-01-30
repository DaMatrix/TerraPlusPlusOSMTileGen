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

import lombok.Getter;
import lombok.NonNull;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.squashfs.SquashfsConstants.*;

/**
 * @author DaPorkchop_
 */
@Getter
public class Metablock {
    protected final char header;
    protected final byte[] data;

    public Metablock(boolean uncompressed, @NonNull byte[] data) {
        checkArg(data.length <= METABLOCK_MAX_SIZE, "metadata block too large: %d (must be at most %d)", data.length, METABLOCK_MAX_SIZE);
        this.header = tochar(uncompressed ? (data.length | METABLOCK_HEADER_UNCOMPRESSED_FLAG) : data.length);
        this.data = data;
    }

    /**
     * @return whether or not this metadata block is uncompressed
     */
    public boolean uncompressed() {
        return (this.header & METABLOCK_HEADER_UNCOMPRESSED_FLAG) != 0;
    }

    /**
     * @return the size of this metadata block on disk (possibly compressed)
     */
    public int data_size() {
        return this.header & METABLOCK_HEADER_DATA_SIZE_MASK;
    }
}
