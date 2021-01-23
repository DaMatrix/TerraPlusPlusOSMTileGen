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

package net.daporkchop.tpposmtilegen.storage.special;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.util.Threading;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * @author DaPorkchop_
 */
public final class StringToBlobDB extends WrappedRocksDB {
    public StringToBlobDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void put(@NonNull DBAccess access, @NonNull String key, @NonNull ByteBuffer value) throws Exception {
        ByteBuf buf = WRITE_BUFFER_CACHE.get().clear();
        buf.writeCharSequence(key, StandardCharsets.US_ASCII);

        access.put(this.column, buf.internalNioBuffer(0, buf.readableBytes()), value);
    }

    public void delete(@NonNull DBAccess access, @NonNull String key) throws Exception {
        access.delete(this.column, key.getBytes(StandardCharsets.US_ASCII));
    }

    public void forEachParallel(@NonNull DBAccess access, @NonNull BiConsumer<String, ByteBuffer> callback) throws Exception {
        @AllArgsConstructor
        class ValueWithKey {
            final byte[] key;
            final byte[] value;
        }

        Threading.<ValueWithKey>iterateParallel(1024,
                c -> {
                    try (RocksIterator itr = access.iterator(this.column)) {
                        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                            c.accept(new ValueWithKey(itr.key(), itr.value()));
                        }
                    }
                },
                v -> callback.accept(new String(v.key, StandardCharsets.US_ASCII), ByteBuffer.wrap(v.value)));
    }

    public long count(@NonNull DBAccess access) throws Exception {
        long count = 0L;
        try (RocksIterator iterator = access.iterator(this.column)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        }
        return count;
    }
}
