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

package net.daporkchop.tpposmtilegen.storage.map;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

/**
 * @author DaPorkchop_
 */
public final class RelationDB extends RocksDBMap<Relation> {
    public RelationDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    @Override
    protected void valueToBytes(@NonNull Relation value, @NonNull ByteBuf dst) {
        value.toBytes(dst);
    }

    @Override
    protected Relation valueFromBytes(long key, @NonNull ByteBuf valueBytes) {
        return new Relation(key, valueBytes);
    }
}
