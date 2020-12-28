/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.util.offheap.lock;

import lombok.experimental.UtilityClass;
import net.daporkchop.lib.unsafe.PUnsafe;

/**
 * A simple read/write lock implemented using off-heap memory.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class OffHeapReadWriteLock {
    public static final long SIZE = 4L;

    private static final int WRITE_SHIFT = 0;
    private static final int READ_SHIFT = 16;

    private static final int WRITE_MASK = ((1 << (READ_SHIFT - WRITE_SHIFT)) - 1) << WRITE_SHIFT;
    private static final int READ_MASK = ((1 << (32 - READ_SHIFT)) - 1) << READ_SHIFT;

    public static void init(long lock) {
        PUnsafe.putInt(lock, 0);
    }

    public static void acquireRead(long lock) {
        while (true) {
            int i = PUnsafe.getIntVolatile(null, lock);
            if ((i & WRITE_MASK) == 0 && PUnsafe.compareAndSwapInt(null, lock, i, i + (1 << READ_SHIFT))) {
                return;
            }
        }
    }

    public static void unlockRead(long lock) {
        int i;
        do {
            i = PUnsafe.getIntVolatile(null, lock);
        } while (!PUnsafe.compareAndSwapInt(null, lock, i, i - (1 << READ_SHIFT)));
    }

    public static void acquireWrite(long lock) {
        while (true) {
            int i = PUnsafe.getIntVolatile(null, lock);
            if ((i & WRITE_MASK) == 0 && PUnsafe.compareAndSwapInt(null, lock, i, i | 1)) {
                //spin until the read locks are released
                while ((i & READ_MASK) != 0) {
                    i = PUnsafe.getIntVolatile(null, lock);
                }
                return;
            }
        }
    }

    public static void unlockWrite(long lock) {
        PUnsafe.putIntVolatile(null, lock, 0);
    }
}
