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

import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.UInt64SetMergeOperator;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * Tracks references between elements.
 *
 * @author DaPorkchop_
 */
public class ReferenceDB extends WrappedRocksDB {
    public ReferenceDB(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void addReference(@NonNull DBWriteAccess access, long id, long referentCombined) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        ByteArrayRecycler mergeOpRecycler = BYTE_ARRAY_RECYCLER_24.get();
        byte[] key = keyArrayRecycler.get();
        byte[] mergeOp = mergeOpRecycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
            UInt64SetMergeOperator.setSingleAdd(mergeOp, referentCombined);
            access.merge(this.column, key, mergeOp);
        } finally {
            mergeOpRecycler.release(mergeOp);
            keyArrayRecycler.release(key);
        }
    }

    public void addReferences(@NonNull DBWriteAccess access, @NonNull LongList ids, long referentCombined) throws Exception {
        int size = ids.size();
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
                long id = ids.getLong(i);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                access.merge(this.column, key, mergeOp);
            }
        } finally {
            mergeOpRecycler.release(mergeOp);
            keyArrayRecycler.release(key);
        }
    }

    public void addReferences(@NonNull DBWriteAccess access, int type, @NonNull LongList ids, long referentCombined) throws Exception {
        int size = ids.size();
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
                long id = Element.addTypeToId(type, ids.getLong(i));
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
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
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
            UInt64SetMergeOperator.setSingleDelete(mergeOp, referentCombined);
            access.merge(this.column, key, mergeOp);
        } finally {
            mergeOpRecycler.release(mergeOp);
            keyArrayRecycler.release(key);
        }
    }

    public void deleteReferencesTo(@NonNull DBWriteAccess access, long combinedId) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = keyArrayRecycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
            access.delete(this.column, key);
        } finally {
            keyArrayRecycler.release(key);
        }
    }

    public void getReferencesTo(@NonNull DBReadAccess access, long combinedId, @NonNull LongList dst) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] key = keyArrayRecycler.get();
        try {
            PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
            byte[] listBytes = access.get(this.column, key);
            if (listBytes != null) {
                checkState(listBytes.length % 8 == 0, listBytes.length);
                for (int i = 0; i < listBytes.length; i += 8) {
                    long val = PUnsafe.getLong(listBytes, PUnsafe.ARRAY_BYTE_BASE_OFFSET + i);
                    dst.add(PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(val) : val);
                }
            }
        } finally {
            keyArrayRecycler.release(key);
        }
    }

    public static class Legacy extends ReferenceDB {
        public Legacy(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
            super(database, column, desc);
        }

        @Override
        public void addReference(@NonNull DBWriteAccess access, long id, long referent) throws Exception {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] key = recycler.get();
            try {
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(referent) : referent);
                access.put(this.column, key, EMPTY_BYTE_ARRAY);
            } finally {
                recycler.release(key);
            }
        }

        @Override
        public void addReferences(@NonNull DBWriteAccess access, @NonNull LongList ids, long referentCombined) throws Exception {
            int size = ids.size();
            if (size == 0) {
                return;
            }

            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] key = recycler.get();
            try {
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(referentCombined) : referentCombined);
                for (int i = 0; i < size; i++) {
                    long id = ids.getLong(i);
                    PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                    access.put(this.column, key, EMPTY_BYTE_ARRAY);
                }
            } finally {
                recycler.release(key);
            }
        }

        @Override
        public void addReferences(@NonNull DBWriteAccess access, int type, @NonNull LongList ids, long referentCombined) throws Exception {
            int size = ids.size();
            if (size == 0) {
                return;
            }

            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] key = recycler.get();
            try {
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(referentCombined) : referentCombined);
                for (int i = 0; i < size; i++) {
                    long id = Element.addTypeToId(type, ids.getLong(i));
                    PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(id) : id);
                    access.put(this.column, key, EMPTY_BYTE_ARRAY);
                }
            } finally {
                recycler.release(key);
            }
        }

        @Override
        public void deleteReference(@NonNull DBWriteAccess access, long combinedId, long referentCombined) throws Exception {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] key = recycler.get();
            try {
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
                PUnsafe.putLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(referentCombined) : referentCombined);
                access.delete(this.column, key);
            } finally {
                recycler.release(key);
            }
        }

        @Override
        public void deleteReferencesTo(@NonNull DBWriteAccess access, long combinedId) throws Exception {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] from = recycler.get();
            byte[] to = recycler.get();
            try {
                PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
                PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
                PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId + 1L) : combinedId + 1L);
                PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
                access.deleteRange(this.column, from, to);
            } finally {
                recycler.release(from);
                recycler.release(to);
            }
        }

        @Override
        public void getReferencesTo(@NonNull DBReadAccess access, long combinedId, @NonNull LongList dst) throws Exception {
            ByteArrayRecycler recycler = BYTE_ARRAY_RECYCLER_16.get();
            byte[] from = recycler.get();
            byte[] to = recycler.get();
            try {
                PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId) : combinedId);
                PUnsafe.putLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
                PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(combinedId + 1L) : combinedId + 1L);
                PUnsafe.putLong(to, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, 0L);
                try (DBIterator iterator = access.iterator(this.column, from, to)) {
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                        byte[] key = iterator.key();

                        //iterating over a transaction can go far beyond the actual iteration bound (rocksdb bug), so we have to manually check
                        checkState(PUnsafe.getLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET) == PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET), "%d != %d",
                                PUnsafe.getLong(from, PUnsafe.ARRAY_BYTE_BASE_OFFSET), PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET));

                        long val = PUnsafe.getLong(key, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L);
                        dst.add(PlatformInfo.IS_LITTLE_ENDIAN ? Long.reverseBytes(val) : val);
                    }
                }
            } finally {
                recycler.release(from);
                recycler.release(to);
            }
        }
    }
}
