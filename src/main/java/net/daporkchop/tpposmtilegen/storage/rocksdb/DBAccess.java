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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author DaPorkchop_
 */
public interface DBAccess extends AutoCloseable {
    //read

    byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception;

    List<byte[]> multiGetAsList(List<ColumnFamilyHandle> columnFamilyHandleList, List<byte[]> keys) throws Exception;

    RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle) throws Exception;

    RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle, ReadOptions options) throws Exception;

    //write

    void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception;

    void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws Exception;

    void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws Exception;

    void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws Exception;

    void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) throws Exception;

    //misc

    long getDataSize() throws Exception;

    void flush() throws Exception;

    void clear() throws Exception;

    void close() throws Exception;

    boolean threadSafe();
}
