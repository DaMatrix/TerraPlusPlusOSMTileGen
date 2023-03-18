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

package net.daporkchop.tpposmtilegen.storage.special;

import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.Threading;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import java.util.BitSet;
import java.util.function.LongConsumer;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class DBLongSet extends WrappedRocksDB {
    public DBLongSet(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void add(@NonNull DBWriteAccess access, long value) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), value);
            access.put(this.column, keyArray, EMPTY_BYTE_ARRAY);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public void remove(@NonNull DBWriteAccess access, long value) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), value);
            access.delete(this.column, keyArray);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public boolean contains(@NonNull DBReadAccess access, long value) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), value);
            return access.contains(this.column, keyArray);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public void forEach(@NonNull DBReadAccess access, @NonNull LongConsumer callback) throws Exception {
        try (DBIterator itr = access.iterator(this.column)) {
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                NativeRocksHelper.KeyValueSlice slice = itr.keyValueSlice();
                checkState(slice.keySize() == 8, slice.keySize());
                checkState(slice.valueSize() == 0, slice.valueSize());

                callback.accept(PUnsafe.getUnalignedLongBE(slice.keyAddr()));
            }
        }
    }

    public void forEachParallel(@NonNull DBReadAccess access, @NonNull LongConsumer callback) throws Exception {
        Threading.iterateParallel(PorkUtil.CPU_COUNT, c -> this.forEach(access, c::accept), callback::accept);
    }
}
