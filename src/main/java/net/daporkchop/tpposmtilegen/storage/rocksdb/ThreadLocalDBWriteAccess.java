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

package net.daporkchop.tpposmtilegen.storage.rocksdb;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.BulkFlushable;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class ThreadLocalDBWriteAccess implements DBWriteAccess {
    @NonNull
    protected final CloseableThreadLocal<DBWriteAccess> delegate;

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.delegate.get().put(columnFamilyHandle, key, value);
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.delegate.get().put(columnFamilyHandle, key, value);
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.delegate.get().merge(columnFamilyHandle, key, value);
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.delegate.get().delete(columnFamilyHandle, key);
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        this.delegate.get().deleteRange(columnFamilyHandle, beginKey, endKey);
    }

    @Override
    public long getDataSize() throws Exception {
        return this.delegate.get().getDataSize();
    }

    @Override
    public boolean isDirty() throws Exception {
        return this.delegate.get().isDirty();
    }

    @Override
    public void flush() throws Exception {
        List<DBWriteAccess> instances = new ArrayList<>();
        this.delegate.forEach(instances::add);

        if (instances.stream().anyMatch(BulkFlushable.class::isInstance)) {
            //do a bulk flush! (we quietly assume that all instances are of the same type)
            ((BulkFlushable<?>) instances.get(0)).bulkFlush(uncheckedCast(instances));
        } else {
            //we can't do a bulk flush, so we'll resort to flushing each instance individually
            instances.forEach((EConsumer<DBWriteAccess>) DBWriteAccess::flush);
        }
    }

    @Override
    public void clear() throws Exception {
        this.delegate.forEach((EConsumer<DBWriteAccess>) DBWriteAccess::clear);
    }

    @Override
    public void close() throws Exception {
        this.delegate.close();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }
}
