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
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.util.persistent.PersistentMap;

import java.nio.file.Path;

/**
 * {@link PersistentMap} for storing {@link Relation}s.
 *
 * @author DaPorkchop_
 */
final class RelationDB extends DB<Long, Relation> {
    public RelationDB(@NonNull Path root, @NonNull String name) throws Exception {
        super(root, name);
    }

    @Override
    protected void keyToBytes(@NonNull Long key, @NonNull ByteBuf dst) {
        dst.writeLong(key);
    }

    @Override
    protected void valueToBytes(@NonNull Relation value, @NonNull ByteBuf dst) {
        value.toBytes(dst);
    }

    @Override
    protected Relation valueFromBytes(@NonNull Long key, @NonNull ByteBuf valueBytes) {
        return new Relation(key, valueBytes);
    }

    @Override
    protected Relation valueFromBytes(@NonNull ByteBuf keyBytes, @NonNull ByteBuf valueBytes) {
        return new Relation(keyBytes.readLong(), valueBytes);
    }
}
