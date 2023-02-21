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

import lombok.NonNull;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.rocksdb.MergeOperator;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class UInt64SetMergeOperator extends MergeOperator {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
        init();
    }

    public static final UInt64SetMergeOperator INSTANCE = new UInt64SetMergeOperator();

    private static native void init();

    private static native long create();

    public UInt64SetMergeOperator() {
        super(create());
    }

    @Override
    protected native void disposeInternal(long handle);

    public static int getSizeBytes(int adds, int dels) {
        return addExact(16, multiplyExact(addExact(adds, dels), 8));
    }

    public static long getSizeBytes(long adds, long dels) {
        return addExact(16L, multiplyExact(addExact(adds, dels), 8L));
    }

    public static void setSingleAdd(@NonNull byte[] arr, long addedValue) {
        checkArg(arr.length == getSizeBytes(1, 0), "%d != %d", arr.length, getSizeBytes(1, 0));
        setNumAddsDels(arr, 1, 0);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(addedValue) : addedValue);
    }

    public static void setSingleDelete(@NonNull byte[] arr, long removedValue) {
        checkArg(arr.length == getSizeBytes(0, 1), "%d != %d", arr.length, getSizeBytes(0, 1));
        setNumAddsDels(arr, 0, 1);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(removedValue) : removedValue);
    }

    public static long getNumAdds(@NonNull byte[] arr) {
        checkArg(arr.length >= 16);
        long value = PUnsafe.getLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET);
        return PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value;
    }

    public static long getIndexedAdd(@NonNull byte[] arr, long index) {
        checkIndex(getNumAdds(arr), index);
        long value = PUnsafe.getLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L + index * 8L);
        return PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value;
    }

    public static long getNumDels(@NonNull byte[] arr) {
        checkArg(arr.length >= 16);
        long value = PUnsafe.getLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L);
        return PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value;
    }

    public static long getIndexedDel(@NonNull byte[] arr, long index) {
        checkIndex(getNumDels(arr), index);
        long value = PUnsafe.getLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L + (getNumAdds(arr) + index) * 8L);
        return PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value;
    }

    public static void setNumAddsDels(@NonNull byte[] arr, int adds, int dels) {
        notNegative(adds, "adds");
        notNegative(dels, "dels");
        checkArg(arr.length == getSizeBytes(adds, dels), "%d != %d", arr.length, getSizeBytes(adds, dels));

        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(adds) : adds);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(dels) : dels);
    }

    public static void setIndexedAdd(@NonNull byte[] arr, long index, long value) {
        checkIndex(getNumAdds(arr), index);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L + index * 8L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value);
    }

    public static void setIndexedDel(@NonNull byte[] arr, long index, long value) {
        checkIndex(getNumDels(arr), index);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L + (getNumAdds(arr) + index) * 8L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value);
    }
}
