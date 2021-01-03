/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2021 DaPorkchop_
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

import lombok.experimental.UtilityClass;

/**
 * Helper methods for converting points to tile locations.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class Tile {
    public static final int TILES_PER_DEGREE = 64;

    public static int coord_point2tile(int pointCoordinate) {
        return Math.floorDiv(pointCoordinate, Point.PRECISION / TILES_PER_DEGREE);
    }

    public static long point2tile(int x, int y) {
        return xy2tilePos(coord_point2tile(x), coord_point2tile(y));
    }

    public static long xy2tilePos(int tileX, int tileY) {
        return interleaveBits(tileX, tileY);
    }

    public static int tileX(long tilePos) {
        return uninterleave(tilePos);
    }

    public static int tileY(long tilePos) {
        return uninterleave(tilePos >>> 1L);
    }

    private static long interleaveBits(int x, int y) {
        x = (x << 1) ^ (x >> 31); //ZigZag encoding
        y = (y << 1) ^ (y >> 31);

        long l = 0L;
        for (int i = 0; i < 32; i++) {
            l |= ((long) (x & (1 << i)) << i) | ((long) (y & (1 << i)) << (i + 1));
        }
        return l;
    }

    private static int uninterleave(long l) {
        int i = 0;
        for (int j = 0; j < 32; j++) {
            i |= ((l >>> (j << 1)) & 1) << j;
        }
        return (i >> 1) ^ -(i & 1);
    }
}
