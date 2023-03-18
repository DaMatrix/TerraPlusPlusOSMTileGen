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
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.annotation.param.NotNegative;
import net.daporkchop.lib.unsafe.PUnsafe;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

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

    public static int memcmp(@NonNull byte[] s1, @NonNull byte[] s2) {
        checkArg(s1.length == s2.length);

        return memcmp0(s1, 0, s2, 0, s1.length);
    }

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

    private static final int MEMCPY_JNI_THRESHOLD = 256;

    private static void memcpyJava(Object dstBase, long dstOffset, Object srcBase, long srcOffset, @NotNegative long n) {
        long pos = 0L;
        while (pos + Long.BYTES <= n) {
            PUnsafe.putUnalignedLong(dstBase, dstOffset + pos, PUnsafe.getUnalignedLong(srcBase, srcOffset + pos));
            pos += Long.BYTES;
        }

        if (pos != n) {
            if (pos + Integer.BYTES <= n) {
                PUnsafe.putUnalignedInt(dstBase, dstOffset + pos, PUnsafe.getUnalignedInt(srcBase, srcOffset + pos));
                pos += Integer.BYTES;
            }

            while (pos < n) {
                PUnsafe.putByte(dstBase, dstOffset + pos, PUnsafe.getByte(srcBase, srcOffset + pos));
                pos++;
            }
        }
    }

    private static native void memcpyNative(long dst, long src, @NotNegative long n);

    public static void memcpy(long dst, long src, @NotNegative long n) {
        if (n <= MEMCPY_JNI_THRESHOLD) {
            memcpyJava(null, dst, null, src, n);
        } else {
            memcpyNative(dst, src, n);
        }
    }

    public static void memcpy(long dst, byte[] src, int srcOffset, @NotNegative int n) {
        checkRangeLen(src.length, srcOffset, n);
        memcpyJava(null, dst, src, PUnsafe.arrayByteElementOffset(srcOffset), n);
    }

    public static void memcpy(byte[] dst, int dstOffset, long src, @NotNegative int n) {
        checkRangeLen(dst.length, dstOffset, n);
        memcpyJava(dst, PUnsafe.arrayByteElementOffset(dstOffset), null, src, n);
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

    public enum Usage {
        MADV_NORMAL,
        MADV_RANDOM,
        MADV_SEQUENTIAL,
        MADV_WILLNEED,
        MADV_DONTNEED,
        MADV_REMOVE,
        MADV_HUGEPAGE,
    }

    private static native void madvise0(long addr, long size, int usage);

    public static void madvise(long addr, long size, @NonNull Memory.Usage usage) {
        madvise0(addr, size, usage.ordinal());
    }

    public static native long malloc(long size) throws OutOfMemoryError;

    public static native long realloc(long addr, long size) throws OutOfMemoryError;

    public static long realloc(long addr, long new_size, long old_size) throws OutOfMemoryError {
        return realloc(addr, new_size);
    }

    public static native void free(long addr);

    public static native void free(long addr, long size);

    public static native void releaseMemoryToSystem();

    @RequiredArgsConstructor
    public enum MapProtection {
        EXEC(PROT_EXEC()),
        READ(PROT_READ()),
        WRITE(PROT_WRITE()),
        NONE(PROT_NONE()),
        READ_WRITE(READ.flags | WRITE.flags),
        READ_WRITE_EXEC(READ_WRITE.flags | EXEC.flags),
        READ_EXEC(READ.flags | EXEC.flags),
        ;

        private static native int PROT_EXEC();

        private static native int PROT_READ();

        private static native int PROT_WRITE();

        private static native int PROT_NONE();

        private final int flags;
    }

    @RequiredArgsConstructor
    public enum MapVisibility {
        SHARED(SHARED()),
        SHARED_VALIDATE(SHARED_VALIDATE()),
        PRIVATE(PRIVATE()),
        ;

        private static native int SHARED();

        private static native int SHARED_VALIDATE();

        private static native int PRIVATE();

        private final int flags;
    }

    @RequiredArgsConstructor
    public enum MapFlags {
        ANONYMOUS(ANONYMOUS()),
        FIXED(FIXED()),
        FIXED_NOREPLACE(FIXED_NOREPLACE()),
        GROWSDOWN(GROWSDOWN()),
        HUGETLB(HUGETLB()),
        HUGE_2MB(HUGE_2MB()),
        HUGE_1GB(HUGE_1GB()),
        LOCKED(LOCKED()),
        NORESERVE(NORESERVE()),
        POPULATE(POPULATE()),
        SYNC(SYNC()),
        ;

        private static native int ANONYMOUS();

        private static native int FIXED();

        private static native int FIXED_NOREPLACE();

        private static native int GROWSDOWN();

        private static native int HUGETLB();

        private static native int HUGE_2MB();

        private static native int HUGE_1GB();

        private static native int LOCKED();

        private static native int NORESERVE();

        private static native int POPULATE();

        private static native int SYNC();

        private final int flags;
    }

    private static native long mmap0(long addr, long length, int prot, int flags, int fd, long offset);

    public static long mmap(long addr, long length, int fd, long offset, @NonNull MapProtection protection, @NonNull MapVisibility visibility, @NonNull MapFlags... flags) {
        return mmap0(addr, length, protection.flags, Stream.of(flags).mapToInt(flag -> flag.flags).reduce(visibility.flags, (a, b) -> a | b), fd, offset);
    }

    @RequiredArgsConstructor
    public enum RemapFlags {
        MAYMOVE(MAYMOVE()),
        FIXED(FIXED()),
        //DONTUNMAP(DONTUNMAP()),
        ;

        private static native int MAYMOVE();

        private static native int FIXED();

        //private static native int DONTUNMAP();

        private final int flags;
    }

    private static native long mremap0(long old_address, long old_size, long new_size, int flags, long new_address);

    public static long mremap(long old_address, long old_size, long new_size, @NonNull RemapFlags... flags) {
        return mremap0(old_address, old_size, new_size, Stream.of(flags).mapToInt(flag -> flag.flags).reduce(0, (a, b) -> a | b), 0L);
    }

    public static long mremap(long old_address, long old_size, long new_size, long new_address, @NonNull RemapFlags... flags) {
        return mremap0(old_address, old_size, new_size, Stream.of(flags).mapToInt(flag -> flag.flags).reduce(0, (a, b) -> a | b), new_address);
    }

    private static native void mprotect0(long addr, long len, int prot);

    public static void mprotect(long addr, long len, @NonNull MapProtection protection) {
        mprotect0(addr, len, protection.flags);
    }

    public static native void munmap(long addr, long length);
}
