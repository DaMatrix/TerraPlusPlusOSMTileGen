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

package net.daporkchop.tpposmtilegen.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.annotation.param.NotNegative;
import net.daporkchop.lib.common.misc.Tuple;
import net.daporkchop.lib.common.reference.cache.Cached;
import sun.security.util.ByteArrayLexOrder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Utils {
    public final Cached<SimpleRecycler<ByteBuf>> IO_BUFFER_RECYCLER = Cached.threadLocal(() -> new SimpleRecycler<ByteBuf>() {
        @Override
        protected ByteBuf newInstance0() {
            return Unpooled.directBuffer();
        }

        @Override
        protected void reset0(@NonNull ByteBuf value) {
            value.clear();
        }

        @Override
        protected boolean hasCapacity() {
            return this.size() < 16;
        }
    });

    private static boolean ALLOW_FORKJOINPOOL = false;

    public static final Comparator<byte[]> BYTES_COMPARATOR = new ByteArrayLexOrder();

    public static final double POINT_DENSITY_LEVEL0_AVERAGE = 2409.331319429213d;
    public static final double POINT_DENSITY_LEVEL0_FIRST_QUARTILE = 927.067380381451d;
    public static final double POINT_DENSITY_LEVEL0_MEDIAN = 1519.78891284316d;
    public static final double POINT_DENSITY_LEVEL0_THIRD_QUARTILE = 2891.26581651644d;

    public static final int MAX_LEVELS = 1;

    public static double minimumDensityAtLevel(int level) {
        //increase by factor of 2 with each level
        return POINT_DENSITY_LEVEL0_FIRST_QUARTILE * (1 << notNegative(level, "level"));
    }

    public static String formatSize(long bytes) {
        if (bytes < 1L << 10L) {
            return bytes + " B";
        } else if (bytes < 1L << 20L) {
            return String.format("%.2f KiB", (double) bytes / (1L << 10L));
        } else if (bytes < 1L << 30L) {
            return String.format("%.2f MiB", (double) bytes / (1L << 20L));
        } else if (bytes < 1L << 40L) {
            return String.format("%.2f GiB", (double) bytes / (1L << 30L));
        } else {
            return String.format("%.2f TiB", (double) bytes / (1L << 40L));
        }
    }

    public void truncate(@NonNull FileChannel dst, @NotNegative long size) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            if (dst.size() >= size) {
                dst.truncate(size);
            } else {
                writeFully(dst, size - 1L, Unpooled.wrappedBuffer(new byte[1]));
            }
            checkState(dst.size() == size);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void writeFully(@NonNull FileChannel dst, @NonNull ByteBuf src) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            while (src.isReadable()) {
                src.readBytes(dst, src.readableBytes());
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void writeFully(@NonNull FileChannel dst, long pos, @NonNull ByteBuf src) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            while (src.isReadable()) {
                pos += src.readBytes(dst, pos, src.readableBytes());
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void readFully(@NonNull FileChannel src, @NonNull ByteBuf dst, int count) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            dst.ensureWritable(count);
            for (int i = 0; i < count; i += dst.writeBytes(src, count - i)) {
                //empty
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void readFully(@NonNull FileChannel src, long pos, @NonNull ByteBuf dst, int count) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            dst.ensureWritable(count);
            for (int i = 0; i < count; i += dst.writeBytes(src, pos + i, count - i)) {
                //empty
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void transferFully(@NonNull FileChannel src, @NonNull FileChannel dst) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            for (long pos = 0L, count = src.position(); pos < count; pos += src.transferTo(pos, count - pos, dst)) {
                //empty
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void padTo4k(@NonNull FileChannel channel) throws IOException {
        long capacity = channel.position();
        if ((capacity & 0xFFFL) != 0L) {
            writeFully(channel, Unpooled.wrappedBuffer(new byte[toInt(4096L - (capacity & 0xFFFL))]));
        }
    }

    public int u16(int val) {
        checkArg((val & 0xFFFF) == val);
        return val;
    }

    public int u16(long val) {
        checkArg((val & 0xFFFFL) == val);
        return (int) val;
    }

    public int u32(long val) {
        checkArg((val & 0xFFFFFFFFL) == val);
        return (int) val;
    }

    public long u64(long val) {
        checkArg(val >= 0L);
        return val;
    }

    /**
     * Interleaves the bits of 2 {@code int}s.
     * <p>
     * Based on <a href="https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN">Bit Twiddling Hacks - Interleave bits by Binary Magic Numbers</a>.
     *
     * @return the interleaved bits
     */
    public static long interleaveBits(int i0, int i1) {
        return spreadBits1(i0) | (spreadBits1(i1) << 1L);
    }

    private static long spreadBits1(long i) {
        //clear upper bits
        i &= (1L << 32L) - 1L;

        //basically magic
        i = (i | (i << 16L)) & 0x0000FFFF0000FFFFL;
        i = (i | (i << 8L)) & 0x00FF00FF00FF00FFL;
        i = (i | (i << 4L)) & 0x0F0F0F0F0F0F0F0FL;
        i = (i | (i << 2L)) & 0x3333333333333333L;
        i = (i | (i << 1L)) & 0x5555555555555555L;
        return i;
    }

    public static int uninterleaveX(long packed) {
        return (int) unspreadBits1(packed);
    }

    public static int uninterleaveY(long packed) {
        return (int) unspreadBits1(packed >> 1L);
    }

    private static long unspreadBits1(long i) {
        //clear useless bits
        i &= 0x5555555555555555L;

        //basically magic
        i = (i | (i >> 1L)) & 0x3333333333333333L;
        i = (i | (i >> 2L)) & 0x0F0F0F0F0F0F0F0FL;
        i = (i | (i >> 4L)) & 0x00FF00FF00FF00FFL;
        i = (i | (i >> 8L)) & 0x0000FFFF0000FFFFL;
        i = (i | (i >> 16L)) & 0x00000000FFFFFFFFL;
        return i;
    }

    public static <A, B> Stream<Tuple<A, B>> zip(@NonNull Stream<A> a, @NonNull Stream<B> b) {
        List<A> aValues = a.collect(Collectors.toList());
        List<B> bValues = b.collect(Collectors.toList());
        checkState(aValues.size() == bValues.size());

        return IntStream.range(0, aValues.size()).mapToObj(i -> new Tuple<>(aValues.set(i, null), bValues.set(i, null)));
    }

    public static boolean allowedToUseForkJoinPool() {
        return ALLOW_FORKJOINPOOL || Thread.currentThread() instanceof ForkJoinWorkerThread; //if we're already on a ForkJoinPool then sure, by all means
    }

    public synchronized static void setAllowForkJoinPool() {
        checkState(!ALLOW_FORKJOINPOOL, "ForkJoinPool already allowed!");
        ALLOW_FORKJOINPOOL = true;
    }
}
