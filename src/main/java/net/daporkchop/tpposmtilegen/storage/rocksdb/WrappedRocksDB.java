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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.ByteBuffer;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public abstract class WrappedRocksDB {
    protected static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    protected static final Ref<ByteArrayRecycler> BYTE_ARRAY_RECYCLER_8 = ThreadRef.soft(() -> new ByteArrayRecycler(8));
    protected static final Ref<ByteArrayRecycler> BYTE_ARRAY_RECYCLER_16 = ThreadRef.soft(() -> new ByteArrayRecycler(16));
    protected static final Ref<ByteArrayRecycler> BYTE_ARRAY_RECYCLER_24 = ThreadRef.soft(() -> new ByteArrayRecycler(24));
    protected static final Ref<ByteBuffer> DIRECT_BUFFER_RECYCLER_8 = ThreadRef.late(() -> ByteBuffer.allocateDirect(8));
    protected static final Ref<ByteBuffer> DIRECT_BUFFER_RECYCLER_16 = ThreadRef.late(() -> ByteBuffer.allocateDirect(16));
    protected static final Ref<ByteBuf> WRITE_BUFFER_CACHE = ThreadRef.late(UnpooledByteBufAllocator.DEFAULT::directBuffer);

    @NonNull
    protected final Database database;
    @NonNull
    protected ColumnFamilyHandle column;
    @NonNull
    protected final ColumnFamilyDescriptor desc;

    { //maybe possibly do something?
        Compiler.compileClass(this.getClass());
    }

    public void clear() throws Exception {
        this.column = this.database.nukeAndReplaceColumnFamily(this.column, this.desc);
    }

    public void compact() throws Exception {
        //forces compaction of the entire column family
        this.database.delegate().compactRange(this.column);
    }

    @RequiredArgsConstructor
    protected static final class ByteArrayRecycler extends SimpleRecycler<byte[]> {
        protected final int size;

        @Override
        protected byte[] newInstance0() {
            return new byte[this.size];
        }

        @Override
        protected void reset0(@NonNull byte[] value) {
            //no-op
        }

        @Override
        protected boolean hasCapacity() {
            return this.size() < 20_000; //don't cache more than 20k arrays
        }
    }
}
