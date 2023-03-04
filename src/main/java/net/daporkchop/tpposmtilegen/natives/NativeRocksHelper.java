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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.rocksdb.AbstractRocksIterator;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * @author DaPorkchop_
 */
public final class NativeRocksHelper {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
        init();
    }

    private static native void init();

    private static final long WRITE_BATCH_HEADER_SIZE = writeBatchHeaderSize0();

    private static native long writeBatchHeaderSize0();

    public static long writeBatchHeaderSize() {
        return WRITE_BATCH_HEADER_SIZE;
    }

    private static native void getKeyAndValueAsView0(long handle, @NonNull KeyValueSlice slice);

    public static void getKeyAndValueAsView(@NonNull AbstractRocksIterator<?> iterator, @NonNull KeyValueSlice slice) {
        getKeyAndValueAsView0(iterator.getNativeHandle(), slice);
    }

    /**
     * @author DaPorkchop_
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class KeyValueSlice {
        public static Comparator<KeyValueSlice> lexOrderKeyComparator() {
            return (a, b) -> {
                int d = Memory.memcmp(a.keyAddr, b.keyAddr, Math.min(a.keySize, b.keySize));
                return d != 0 ? d : Integer.compare(a.keySize, b.keySize);
            };
        }

        private long keyAddr;
        private int keySize;

        private long valueAddr;
        private int valueSize;

        private void set(long keyAddr, long keySize, long valueAddr, long valueSize) {
            this.keyAddr = keyAddr;
            this.keySize = Math.toIntExact(keySize);
            this.valueAddr = valueAddr;
            this.valueSize = Math.toIntExact(valueSize);
        }
    }

    private static native void writeBatchMerge0(long handle, long column_family, long keyAddr, int keySize, long valueAddr, int valueSize) throws RocksDBException;

    public static void merge(@NonNull WriteBatch batch, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws RocksDBException {
        writeBatchMerge0(batch.getNativeHandle(), columnFamilyHandle.getNativeHandle(),
                PUnsafe.pork_directBufferAddress(key), key.remaining(),
                PUnsafe.pork_directBufferAddress(value), value.remaining());
        key.position(key.limit());
        value.position(value.limit());
    }

    private static native void sstFileWriterMerge0(long handle, long keyAddr, int keySize, long valueAddr, int valueSize) throws RocksDBException;

    public static void merge(@NonNull SstFileWriter writer, @NonNull ByteBuffer key, @NonNull ByteBuffer value) throws RocksDBException {
        sstFileWriterMerge0(writer.getNativeHandle(),
                PUnsafe.pork_directBufferAddress(key), key.remaining(),
                PUnsafe.pork_directBufferAddress(value), value.remaining());
        key.position(key.limit());
        value.position(value.limit());
    }
}
