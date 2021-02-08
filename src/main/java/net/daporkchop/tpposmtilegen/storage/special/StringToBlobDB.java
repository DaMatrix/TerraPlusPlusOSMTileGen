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
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * @author DaPorkchop_
 */
public final class StringToBlobDB extends WrappedRocksDB {
    static byte[] next(@NonNull byte[] curr) {
        int len = curr.length;
        byte[] next = new byte[len + 1];
        System.arraycopy(curr, 0, next, 0, len - 1);
        next[len - 1] = (byte) '0';
        next[len] = (byte) '/';
        return next;
    }

    public StringToBlobDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void put(@NonNull DBAccess access, @NonNull String key, @NonNull ByteBuffer value) throws Exception {
        ByteBuf buf = WRITE_BUFFER_CACHE.get().clear();
        buf.writeCharSequence(key, StandardCharsets.US_ASCII);

        access.put(this.column, buf.internalNioBuffer(0, buf.readableBytes()), value);
    }

    public void putHeap(@NonNull DBAccess access, @NonNull String key, @NonNull ByteBuffer value) throws Exception {
        byte[] arr = value.array();
        if (value.arrayOffset() + value.position() != 0 || value.remaining() != arr.length) {
            arr = Arrays.copyOfRange(arr, value.arrayOffset() + value.position(), value.remaining());
        }

        access.put(this.column, key.getBytes(StandardCharsets.US_ASCII), arr);
    }

    public void delete(@NonNull DBAccess access, @NonNull String key) throws Exception {
        access.delete(this.column, key.getBytes(StandardCharsets.US_ASCII));
    }

    public ByteBuffer get(@NonNull DBAccess access, @NonNull String key) throws Exception {
        byte[] arr = access.get(this.column, key.getBytes(StandardCharsets.US_ASCII));
        return arr != null ? ByteBuffer.wrap(arr) : null;
    }

    public List<String> listChildren(@NonNull DBAccess access, @NonNull String in) throws Exception { //my god this is bad
        if (!in.endsWith("/")) {
            in += '/';
        }
        boolean root = "/".equals(in);
        if (root) {
            in = "";
        }

        byte[] lowKey = in.getBytes(StandardCharsets.US_ASCII);
        int prefixLength = in.length();

        try (Slice lowerBound = root ? null : new Slice(lowKey);
             Slice upperBound = root ? null : new Slice(next(lowKey));
             ReadOptions options = root ? null : new ReadOptions(Database.READ_OPTIONS)
                     .setIterateLowerBound(lowerBound)
                     .setIterateUpperBound(upperBound);
             RocksIterator itr = access.iterator(this.column, root ? Database.READ_OPTIONS : options)) {
            List<String> children = new ArrayList<>();
            for (itr.seekToFirst(); itr.isValid(); ) {
                byte[] key = itr.key();
                String child = new String(key, prefixLength, key.length - prefixLength, StandardCharsets.US_ASCII);
                boolean directory = child.indexOf('/') >= 0;
                if (directory) {
                    child = child.substring(0, child.indexOf('/') + 1);
                }
                children.add(child);
                if (directory) {
                    itr.seek(next((in + child).getBytes(StandardCharsets.US_ASCII)));
                } else {
                    itr.next();
                }
            }
            return children.isEmpty() ? null : children;
        }
    }

    public void forEach(@NonNull DBAccess access, @NonNull BiConsumer<String, ByteBuffer> callback) throws Exception {
        try (RocksIterator itr = access.iterator(this.column)) {
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                callback.accept(new String(itr.key(), StandardCharsets.US_ASCII), ByteBuffer.wrap(itr.value()));
            }
        }
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
