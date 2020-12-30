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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.NonNull;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

/**
 * @author DaPorkchop_
 */
public abstract class DB<K, V> implements AutoCloseable {
    protected static final FastThreadLocal<ByteBuf> WRITE_BUFFER_POOL = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            return UnpooledByteBufAllocator.DEFAULT.heapBuffer();
        }
    };

    protected final RocksDB delegate;

    public DB(@NonNull File file) throws RocksDBException {
        this.delegate = RocksDB.open(file.toString());
    }

    public void put(@NonNull K key, @NonNull V value) throws RocksDBException {
        ByteBuf buf = WRITE_BUFFER_POOL.get().clear();
        int keyStart = buf.writerIndex();
        this.keyToBytes(key, buf);
        int valueStart = buf.writerIndex();
        int keyLen = valueStart - keyStart;
        this.valueToBytes(value, buf);
        int valueLen = buf.writerIndex() - valueStart;
        byte[] arr = buf.array();
        int off = buf.arrayOffset();
        this.delegate.put(arr, off + keyStart, keyLen, arr, off + valueStart, valueLen);
    }

    @Override
    public void close() {
        this.delegate.close();
    }

    protected abstract void keyToBytes(@NonNull K key, @NonNull ByteBuf dst);

    protected abstract void valueToBytes(@NonNull V value, @NonNull ByteBuf dst);

    protected abstract V valueFromBytes(@NonNull byte[] keyBytes, @NonNull byte[] valueBytes);
}
