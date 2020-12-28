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

package net.daporkchop.tpposmtilegen.util.offheap;

import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.Bounds2d;

/**
 * Off-heap implementation of basic geometry types.
 *
 * @author DaPorkchop_
 */
public enum OffHeapGeometry {
    /*
     * struct vec2 {
     *   double x;
     *   double z;
     * };
     */

    POINT {
        /*
         * struct Point {
         *   vec2 pos;
         * };
         */

        @Override
        public Bounds2d bounds(long addr) {
            double x = PUnsafe.getDouble(addr + 0L);
            double z = PUnsafe.getDouble(addr + 8L);
            return Bounds2d.of(x, x, z, z);
        }
    };

    private static final OffHeapGeometry[] VALUES = values();

    /**
     * Gets the {@link OffHeapGeometry} with the given ordinal.
     *
     * @param ordinal the ordinal
     * @return the {@link OffHeapGeometry} with the given ordinal
     */
    public static OffHeapGeometry fromOrdinal(int ordinal) {
        return VALUES[ordinal];
    }

    /**
     * Gets the minimum bounding box of the given geometry object.
     *
     * @param addr pointer to the beginning of the object
     * @return the minimum bounding box of the given geometry object
     */
    public abstract Bounds2d bounds(long addr);
}
