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
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.unsafe.PUnsafe;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Memory {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
    }

    private static native int memcmp0(@NonNull byte[] s1, int offset1, @NonNull byte[] s2, int offset2, int n);

    private static native int memcmp0(@NonNull byte[] s1, int offset1, long s2, int n);

    private static native int memcmp0(long s1, @NonNull byte[] s2, int offset2, int n);

    private static native int memcmp0(long s1, long s2, long n);

    public static int memcmp(@NonNull byte[] s1, int offset1, @NonNull byte[] s2, int offset2, int n) {
        notNegative(n);
        checkArrayRange(s1, offset1, n);
        checkArrayRange(s2, offset2, n);

        return memcmp0(s1, offset1, s2, offset2, n);
    }

    public static int memcmp(@NonNull byte[] s1, int offset1, @NonNull ByteBuffer s2, int n) {
        notNegative(n);
        checkArrayRange(s1, offset1, n);
        checkBufferRange(s2, n);

        return s2.hasArray()
                ? memcmp0(s1, offset1, s2.array(), s2.arrayOffset() + s2.position(), n)
                : memcmp0(s1, offset1, PUnsafe.pork_directBufferAddress(s2) + s2.position(), n);
    }

    public static int memcmp(@NonNull byte[] s1, int offset1, long s2, int n) {
        notNegative(n);
        checkArrayRange(s1, offset1, n);

        return memcmp0(s1, offset1, s2, n);
    }

    public static int memcmp(@NonNull ByteBuffer s1, @NonNull byte[] s2, int offset2, int n) {
        notNegative(n);
        checkBufferRange(s1, n);
        checkArrayRange(s2, offset2, n);

        return s1.hasArray()
                ? memcmp0(s1.array(), s1.arrayOffset() + s1.position(), s2, offset2, n)
                : memcmp0(PUnsafe.pork_directBufferAddress(s1) + s1.position(), s2, offset2, n);
    }

    public static int memcmp(@NonNull ByteBuffer s1, @NonNull ByteBuffer s2, int n) {
        notNegative(n);
        checkBufferRange(s1, n);
        checkBufferRange(s2, n);

        if (s1.hasArray()) {
            return s2.hasArray()
                    ? memcmp0(s1.array(), s1.arrayOffset() + s1.position(), s2.array(), s2.arrayOffset() + s2.position(), n)
                    : memcmp0(s1.array(), s1.arrayOffset() + s1.position(), PUnsafe.pork_directBufferAddress(s2) + s2.position(), n);
        } else {
            return s2.hasArray()
                    ? memcmp0(PUnsafe.pork_directBufferAddress(s1) + s1.position(), s2.array(), s2.arrayOffset() + s2.position(), n)
                    : memcmp0(PUnsafe.pork_directBufferAddress(s1) + s1.position(), PUnsafe.pork_directBufferAddress(s2) + s2.position(), n);
        }
    }

    public static int memcmp(@NonNull ByteBuffer s1, long s2, int n) {
        notNegative(n);
        checkBufferRange(s1, n);

        return s1.hasArray()
                ? memcmp0(s1.array(), s1.arrayOffset() + s1.position(), s2, n)
                : memcmp0(PUnsafe.pork_directBufferAddress(s1) + s1.position(), s2, n);
    }

    public static int memcmp(long s1, @NonNull byte[] s2, int offset2, int n) {
        notNegative(n);
        checkArrayRange(s2, offset2, n);

        return memcmp0(s1, s2, offset2, n);
    }

    public static int memcmp(long s1, @NonNull ByteBuffer s2, int n) {
        notNegative(n);
        checkBufferRange(s2, n);

        return s2.hasArray()
                ? memcmp0(s1, s2.array(), s2.arrayOffset() + s2.position(), n)
                : memcmp0(s1, PUnsafe.pork_directBufferAddress(s2) + s2.position(), n);
    }

    public static int memcmp(long s1, long s2, long n) {
        return memcmp0(s1, s2, notNegative(n));
    }

    private static void checkArrayRange(byte[] array, int offset, int n) {
        if (offset < 0 || offset + n > array.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    private static void checkBufferRange(ByteBuffer buffer, int n) {
        if (buffer.remaining() < n) {
            throw new BufferUnderflowException();
        }
    }
}
