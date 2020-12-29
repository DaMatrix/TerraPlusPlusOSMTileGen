/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.storage;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString(callSuper = true)
public final class Way extends Element {
    @NonNull
    protected long[] nodes;

    public Way(long id, Map<String, String> tags, @NonNull long[] nodes) {
        super(id, tags);

        this.nodes = nodes;
    }

    public Way(long id, byte[] data) {
        super(id, data);
    }

    public Way tags(@NonNull Map<String, String> tags) {
        super.tags = tags;
        return this;
    }

    @Override
    protected void toByteArray0(ByteBuf dst) {
        dst.writeInt(this.nodes.length);
        for (long node : this.nodes) {
            dst.writeLong(node);
        }
    }

    @Override
    public Way fromByteArray(@NonNull byte[] data) {
        super.fromByteArray(data);
        return this;
    }

    @Override
    protected void fromByteArray0(ByteBuf src) {
        int count = src.readInt();
        this.nodes = new long[count];
        for (int i = 0; i < count; i++) {
            this.nodes[i] = src.readLong();
        }
    }
}
