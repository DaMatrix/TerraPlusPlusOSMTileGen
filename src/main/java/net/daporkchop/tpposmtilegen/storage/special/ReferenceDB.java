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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.UInt64SetMergeOperator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.DuplicatedList;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * Tracks references between elements.
 *
 * @author DaPorkchop_
 */
public final class ReferenceDB extends WrappedRocksDB {
    public ReferenceDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void addReference(@NonNull DBWriteAccess access, long combinedId, long referentCombined) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        ByteArrayRecycler mergeOpRecycler = BYTE_ARRAY_RECYCLER_24.get();
        byte[] key = keyArrayRecycler.get();
        byte[] mergeOp = mergeOpRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), combinedId);
            UInt64SetMergeOperator.setSingleAdd(mergeOp, referentCombined);
            access.merge(this.column, key, mergeOp);
        } finally {
            mergeOpRecycler.release(mergeOp);
            keyArrayRecycler.release(key);
        }
    }

    public void addReferences(@NonNull DBWriteAccess access, @NonNull LongList combinedIds, long referentCombined) throws Exception {
        int size = combinedIds.size();
        if (size == 0) {
            return;
        }

        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        ByteArrayRecycler mergeOpRecycler = BYTE_ARRAY_RECYCLER_24.get();
        byte[] key = keyArrayRecycler.get();
        byte[] mergeOp = mergeOpRecycler.get();
        try {
            UInt64SetMergeOperator.setSingleAdd(mergeOp, referentCombined);
            for (int i = 0; i < size; i++) {
                long id = combinedIds.getLong(i);
                PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), id);
                access.merge(this.column, key, mergeOp);
            }
        } finally {
            mergeOpRecycler.release(mergeOp);
            keyArrayRecycler.release(key);
        }
    }

    public void deleteReference(@NonNull DBWriteAccess access, long combinedId, long referentCombined) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        ByteArrayRecycler mergeOpRecycler = BYTE_ARRAY_RECYCLER_24.get();
        byte[] key = keyArrayRecycler.get();
        byte[] mergeOp = mergeOpRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), combinedId);
            UInt64SetMergeOperator.setSingleDelete(mergeOp, referentCombined);
            access.merge(this.column, key, mergeOp);
        } finally {
            mergeOpRecycler.release(mergeOp);
            keyArrayRecycler.release(key);
        }
    }

    public void deleteAllReferencesTo(@NonNull DBWriteAccess access, long combinedId) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), combinedId);
            access.delete(this.column, key);
        } finally {
            keyArrayRecycler.release(key);
        }
    }

    public void getReferencesTo(@NonNull DBReadAccess access, long combinedId, @NonNull LongList dst) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), combinedId);
            byte[] listBytes = access.get(this.column, key);
            if (listBytes != null) {
                checkState(listBytes.length % 8 == 0, listBytes.length);
                for (int i = 0; i < listBytes.length; i += 8) {
                    dst.add(PUnsafe.getUnalignedLongLE(listBytes, PUnsafe.arrayByteElementOffset(i)));
                }
            }
        } finally {
            keyArrayRecycler.release(key);
        }
    }

    public LongList getReferencesTo(@NonNull DBReadAccess access, long combinedId) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(key, PUnsafe.arrayByteElementOffset(0), combinedId);
            byte[] listBytes = access.get(this.column, key);
            if (listBytes != null) {
                checkState(listBytes.length % 8 == 0, listBytes.length);
                long[] out = new long[listBytes.length / 8];
                for (int i = 0; i < listBytes.length; i += 8) {
                    out[i / 8] = PUnsafe.getUnalignedLongLE(listBytes, PUnsafe.arrayByteElementOffset(i));
                }
                return LongArrayList.wrap(out);
            }
            return LongLists.EMPTY_LIST;
        } finally {
            keyArrayRecycler.release(key);
        }
    }

    public List<LongList> getReferencesTo(@NonNull DBReadAccess access, @NonNull LongList combinedIds) throws Exception {
        int size = combinedIds.size();
        if (size == 0) {
            return Collections.emptyList();
        } else if (size > 10000) { //split into smaller gets (prevents what i can only assume is a rocksdbjni bug where it will throw an NPE when requesting too many elements at once
            List<LongList> dst = new ArrayList<>(size);
            for (int i = 0; i < size; i += 10000) {
                dst.addAll(this.getReferencesTo(access, combinedIds.subList(i, min(i + 10000, size))));
            }
            return dst;
        }

        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        List<byte[]> keyBytes = new ArrayList<>(size);
        List<byte[]> valueBytes;
        try {
            //serialize keys to bytes
            for (int i = 0; i < size; i++) {
                byte[] keyArray = keyArrayRecycler.get();
                long key = combinedIds.getLong(i);
                PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
                keyBytes.add(keyArray);
            }

            //look up values from key
            valueBytes = access.multiGetAsList(new DuplicatedList<>(this.column, size), keyBytes);
        } finally {
            keyBytes.forEach(keyArrayRecycler::release);
        }

        //re-use list that was previously used for storing encoded keys and store deserialized values in it
        keyBytes.clear();
        List<LongList> values = uncheckedCast(keyBytes);

        for (int i = 0; i < size; i++) {
            byte[] value = valueBytes.get(i);
            if (value != null) {
                checkState(value.length % 8 == 0, value.length);
                long[] out = new long[value.length / 8];
                for (int j = 0; j < value.length; j += 8) {
                    out[j / 8] = PUnsafe.getUnalignedLongLE(value, PUnsafe.arrayByteElementOffset(j));
                }
                values.add(LongArrayList.wrap(out));
            } else {
                values.add(LongLists.EMPTY_LIST);
            }
        }
        return values;
    }

    public void forEachKey(@NonNull DBReadAccess access, @NonNull LongConsumer action) throws Exception {
        try (DBIterator iterator = access.iterator(this.column)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                action.accept(PUnsafe.getUnalignedLongBE(iterator.key(), PUnsafe.arrayByteElementOffset(0)));
            }
        }
    }
}
