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

package net.daporkchop.tpposmtilegen.storage.db;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.storage.Node;
import net.daporkchop.tpposmtilegen.util.map.PersistentMap;

import java.nio.file.Path;

/**
 * {@link PersistentMap} for storing {@link Node}s.
 *
 * @author DaPorkchop_
 */
public final class NodeDB extends DB<Long, Node> {
    public NodeDB(@NonNull Path root, @NonNull String name) throws Exception {
        super(root, name);
    }

    @Override
    protected void keyToBytes(@NonNull Long key, @NonNull ByteBuf dst) {
        dst.writeLong(key);
    }

    @Override
    protected void valueToBytes(@NonNull Node value, @NonNull ByteBuf dst) {
        value.toBytes(dst);
    }

    @Override
    protected Node valueFromBytes(@NonNull Long key, @NonNull ByteBuf valueBytes) {
        return new Node(key, valueBytes);
    }

    @Override
    protected Node valueFromBytes(@NonNull ByteBuf keyBytes, @NonNull ByteBuf valueBytes) {
        return new Node(keyBytes.readLong(), valueBytes);
    }
}
