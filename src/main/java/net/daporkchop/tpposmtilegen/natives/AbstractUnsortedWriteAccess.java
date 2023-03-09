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

package net.daporkchop.tpposmtilegen.natives;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public abstract class AbstractUnsortedWriteAccess implements DBWriteAccess {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
    }

    protected final Storage storage;

    @Getter
    protected final ColumnFamilyHandle columnFamilyHandle;
    protected final String columnFamilyName;

    protected final Logger logger;

    protected volatile boolean flushing = false;

    public AbstractUnsortedWriteAccess(@NonNull Storage storage, @NonNull ColumnFamilyHandle columnFamilyHandle) throws Exception {
        this.storage = storage;
        this.columnFamilyHandle = columnFamilyHandle;
        this.columnFamilyName = new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8);

        this.logger = Logging.logger.channel(this.columnFamilyName);
    }

    protected void checkWriteOk(@NonNull ColumnFamilyHandle columnFamilyHandle) {
        checkArg(columnFamilyHandle == this.columnFamilyHandle, "may only write to this column family");
        checkState(!this.flushing, "currently flushing?!?");
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key, @NonNull byte[] value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] key) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRange(@NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull byte[] beginKey, @NonNull byte[] endKey) throws Exception {
        this.checkWriteOk(columnFamilyHandle);
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized final void flush() throws Exception {
        if (!this.flushing) {
            this.flushing = true;

            this.flush0();
        } else {
            this.logger.warn("attempting to flush again:");
        }
    }

    protected abstract void flush0() throws Exception;

    @Override
    public void clear() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        this.flush();
    }

    @Override
    public boolean threadSafe() {
        return true;
    }
}
