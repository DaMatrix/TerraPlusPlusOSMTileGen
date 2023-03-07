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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import net.daporkchop.lib.common.annotation.param.NotNegative;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.DuplicatedList;
import org.rocksdb.AbstractRocksIterator;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

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
    @NoArgsConstructor
    @Getter
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

    /**
     * @author DaPorkchop_
     */
    @Getter
    private static final class OffHeapSliceArray implements AutoCloseable {
        private static final long SIZE = SIZE();

        private static final long DATA_OFFSET = DATA_OFFSET();
        private static final long DATA_SIZE = DATA_SIZE();

        private static final long SIZE_OFFSET = SIZE_OFFSET();
        private static final long SIZE_SIZE = SIZE_SIZE();

        static {
            checkState(DATA_SIZE == PUnsafe.addressSize());
            checkState(SIZE_SIZE == PUnsafe.addressSize());
        }

        private static native long SIZE();

        private static native long DATA_OFFSET();

        private static native long DATA_SIZE();

        private static native long SIZE_OFFSET();

        private static native long SIZE_SIZE();

        public static long allocSliceArray(@NotNegative long length) {
            return Memory.malloc(multiplyExact(length, SIZE));
        }

        public static void freeSliceArray(long arrayAddr, @NotNegative long length) {
            Memory.free(arrayAddr, length * SIZE);
        }

        public static void setSlice(long addr, long data, @NotNegative long size) {
            PUnsafe.putAddress(addr + DATA_OFFSET, data);
            PUnsafe.putAddress(addr + SIZE_OFFSET, size);
        }

        public static void setSliceArrayElement(long arrayAddr, @NotNegative long index, long data, @NotNegative long size) {
            setSlice(arrayAddr + index * SIZE, data, size);
        }

        public static long getSliceData(long addr) {
            return PUnsafe.getAddress(addr + DATA_OFFSET);
        }

        public static long getSliceSize(long addr) {
            return PUnsafe.getAddress(addr + DATA_SIZE);
        }

        public static long getSliceArrayElementData(long arrayAddr, @NotNegative long index) {
            return getSliceData(arrayAddr + index * SIZE);
        }

        public static long getSliceArrayElementSize(long arrayAddr, @NotNegative long index) {
            return getSliceSize(arrayAddr + index * SIZE);
        }

        private final long dataAddr;
        private final long length;

        public OffHeapSliceArray(@NotNegative long length) {
            this.dataAddr = allocSliceArray(notNegative(length, "length"));
            this.length = length;
        }

        public void set(@NotNegative long index, long data, long size) {
            setSliceArrayElement(this.dataAddr, checkIndex(this.length, index), data, size);
        }

        public long getData(@NotNegative long index) {
            return getSliceArrayElementData(this.dataAddr, checkIndex(this.length, index));
        }

        public long getSize(@NotNegative long index) {
            return getSliceArrayElementSize(this.dataAddr, checkIndex(this.length, index));
        }

        @Override
        public void close() {
            freeSliceArray(this.dataAddr, this.length);
        }
    }

    private static native byte[][] multiGetToArrays0(long dbHandle, long options_handle, long column_family, int num_keys, long key_slices, boolean sorted_input) throws RocksDBException;

    public static List<byte[]> multiGetAsList(@NonNull RocksDB db, @NonNull ReadOptions readOptions, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull List<byte[]> keys) throws RocksDBException {
        return multiGetAsList(db, readOptions, columnFamilyHandle, keys, false);
    }

    public static List<byte[]> multiGetAsList(@NonNull RocksDB db, @NonNull ReadOptions readOptions, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull List<byte[]> keys, boolean sortedInput) throws RocksDBException {
        int size = keys.size();
        if (size == 0) {
            return Collections.emptyList();
        }

        long totalKeyLength = keys.stream().mapToLong(arr -> arr.length).sum();
        long keysAddr = Memory.malloc(totalKeyLength);
        try (OffHeapSliceArray keySlices = new OffHeapSliceArray(size)) {
            { //copy keys off-heap
                long currentKeysAddr = keysAddr;
                for (int i = 0; i < size; i++) {
                    byte[] key = keys.get(i);
                    keySlices.set(i, currentKeysAddr, key.length);
                    Memory.memcpy(currentKeysAddr, key, 0, key.length);
                    currentKeysAddr += key.length;
                }
            }

            return Arrays.asList(multiGetToArrays0(db.getNativeHandle(), readOptions.getNativeHandle(), columnFamilyHandle.getNativeHandle(), size, keySlices.dataAddr, sortedInput));
        } finally {
            Memory.free(keysAddr, totalKeyLength);
        }
    }

    public static List<byte[]> multiGetAsList(@NonNull RocksDB db, @NonNull ReadOptions readOptions, @NonNull List<ColumnFamilyHandle> columnFamilyHandles, @NonNull List<byte[]> keys) throws RocksDBException {
        return multiGetAsList(db, readOptions, columnFamilyHandles, keys, false);
    }

    public static List<byte[]> multiGetAsList(@NonNull RocksDB db, @NonNull ReadOptions readOptions, @NonNull List<ColumnFamilyHandle> columnFamilyHandles, @NonNull List<byte[]> keys, boolean sortedInput) throws RocksDBException {
        if (columnFamilyHandles instanceof DuplicatedList) { //all column families are the same! use the optimized path
            return columnFamilyHandles.isEmpty() ? Collections.emptyList() : multiGetAsList(db, readOptions, columnFamilyHandles.get(0), keys, sortedInput);
        }

        return db.multiGetAsList(readOptions, columnFamilyHandles, keys);
    }
}
