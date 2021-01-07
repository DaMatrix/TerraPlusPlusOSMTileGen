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

package net.daporkchop.tpposmtilegen.storage.rocksdb;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public class WriteBatch extends org.rocksdb.WriteBatch {
    @NonNull
    protected final RocksDB delegate;

    @Override
    @Deprecated
    public void put(byte[] key, byte[] value) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void merge(byte[] key, byte[] value) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void remove(byte[] key) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void delete(byte[] key) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void singleDelete(byte[] key) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void remove(ByteBuffer key) throws RocksDBException {
        throw new UnsupportedOperationException();
    }

    public void flush() {
        this.flush(Database.SYNC_WRITE_OPTIONS);
    }

    public void flush(@NonNull WriteOptions writeOptions) {
        try {
            this.delegate.write(writeOptions, this);
        } catch (RocksDBException e) {
            PUnsafe.throwException(e);
        } finally {
            this.clear();
        }
    }

    @Override
    public void close() {
        this.flush();

        super.close();
    }
}
