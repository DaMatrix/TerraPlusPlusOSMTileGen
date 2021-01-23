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
import net.daporkchop.tpposmtilegen.geometry.Point;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * Helper methods for converting points to tile locations.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class Tile {
    public static final int TILES_PER_DEGREE = 64;
    public static final int TILE_SIZE_POINT_SCALE = Point.PRECISION / TILES_PER_DEGREE;

    public static final int TX_OFFSET = 180 * TILES_PER_DEGREE;
    public static final int TY_OFFSET = 90 * TILES_PER_DEGREE;

    public static int point2tile(int pointCoordinate) {
        return Math.floorDiv(pointCoordinate, TILE_SIZE_POINT_SCALE);
    }

    public static int tile2point(int tileCoordinate) {
        return tileCoordinate * TILE_SIZE_POINT_SCALE;
    }

    public static long point2tile(int x, int y) {
        return xy2tilePos(point2tile(x), point2tile(y));
    }

    public static long xy2tilePos(int tileX, int tileY) {
        tileX += TX_OFFSET;
        tileY += TY_OFFSET;
        checkArg(tileX >= 0 && tileX < 65536, "tileX: %d", tileX);
        checkArg(tileY >= 0 && tileY < 65536, "tileY: %d", tileY);
        return ((Short.reverseBytes((short) tileY) & 0xFFFFL) << 48L) | ((Short.reverseBytes((short) tileX) & 0xFFFFL) << 32L);
    }

    public static int tileX(long tilePos) {
        return toInt(Short.reverseBytes((short) (tilePos >>> 32L)) & 0xFFFF) - TX_OFFSET;
    }

    public static int tileY(long tilePos) {
        return toInt(Short.reverseBytes((short) (tilePos >>> 48L)) & 0xFFFF) - TY_OFFSET;
    }
}
