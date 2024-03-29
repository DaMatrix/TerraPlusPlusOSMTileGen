/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.storage.rocksdb.access;

import lombok.NonNull;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Snapshot;

import java.util.List;
import java.util.Optional;

/**
 * @author DaPorkchop_
 */
public interface DBReadAccess extends DBBaseAccess {
    //read

    byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception;

    List<@NonNull byte[]> multiGetAsList(@NonNull List<@NonNull ColumnFamilyHandle> columnFamilyHandleList, @NonNull List<@NonNull byte[]> keys) throws Exception;

    boolean contains(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception;

    DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle) throws Exception;

    DBIterator iterator(@NonNull ColumnFamilyHandle columnFamilyHandle, byte[] fromInclusive, byte[] toExclusive) throws Exception;

    /**
     * @return {@code true} if this instance performs reads directly from the underlying database. Should return false for any implementations which could return
     *         buffered, unwritten data
     */
    default boolean isDirectRead() {
        return false;
    }

    default Optional<Snapshot> internalSnapshot() {
        return Optional.empty();
    }
}
