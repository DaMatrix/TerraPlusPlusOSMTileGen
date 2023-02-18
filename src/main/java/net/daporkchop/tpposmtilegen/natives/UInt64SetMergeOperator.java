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

    public static int getArraySizeBytes(int adds, int dels) {
        return 16 + (adds + dels) * 8;
    }

    public static void setSingleAdd(@NonNull byte[] arr, long addedValue) {
        checkArg(arr.length == getArraySizeBytes(1, 0), "%d != %d", arr.length, getArraySizeBytes(1, 0));
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(1L) : 1L);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(0L) : 0L);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(addedValue) : addedValue);
    }

    public static void setSingleDelete(@NonNull byte[] arr, long removedValue) {
        checkArg(arr.length == getArraySizeBytes(0, 1), "%d != %d", arr.length, getArraySizeBytes(0, 1));
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(0L) : 0L);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 8L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(1L) : 1L);
        PUnsafe.putLong(arr, PUnsafe.ARRAY_BYTE_BASE_OFFSET + 16L, PlatformInfo.IS_BIG_ENDIAN ? Long.reverseBytes(removedValue) : removedValue);
    }
}
