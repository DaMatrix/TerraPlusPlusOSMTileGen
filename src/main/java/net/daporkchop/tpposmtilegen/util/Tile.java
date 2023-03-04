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

import lombok.experimental.UtilityClass;
import net.daporkchop.tpposmtilegen.geometry.Point;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static net.daporkchop.tpposmtilegen.util.Utils.*;

/**
 * Helper methods for converting points to tile locations.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class Tile {
    private static final int TILES_PER_DEGREE_LEVEL0 = 64;
    private static final int TILE_SIZE_POINT_SCALE_LEVEL0 = Point.PRECISION / TILES_PER_DEGREE_LEVEL0;

    public static final int[] TILE_SIZE_POINT_SCALE = IntStream.range(0, MAX_LEVELS).map(lvl -> TILE_SIZE_POINT_SCALE_LEVEL0 * (1 << lvl)).toArray();

    public static int point2tile(int level, int pointCoordinate) {
        return Math.floorDiv(pointCoordinate, TILE_SIZE_POINT_SCALE[level]);
    }

    public static int tile2point(int level, int tileCoordinate) {
        return tileCoordinate * TILE_SIZE_POINT_SCALE[level];
    }

    public static long point2tile(int level, int x, int y) {
        return xy2tilePos(point2tile(level, x), point2tile(level, y));
    }

    public static long xy2tilePos(int tileX, int tileY) {
        return Utils.interleaveBits(tileX, tileY);
    }

    public static int tileX(long tilePos) {
        return Utils.uninterleaveX(tilePos);
    }

    public static int tileY(long tilePos) {
        return Utils.uninterleaveY(tilePos);
    }

    public static Bounds2d levelBounds(int level) {
        return Bounds2d.of(
                point2tile(level, -180 * Point.PRECISION),
                point2tile(level, 180 * Point.PRECISION),
                point2tile(level, -90 * Point.PRECISION),
                point2tile(level, 90 * Point.PRECISION));
    }

    public static LongStream levelTiles(int level, boolean parallel) {
        Bounds2d bounds = levelBounds(level);
        return (parallel ? IntStream.rangeClosed(bounds.minX(), bounds.maxX()).parallel() : IntStream.rangeClosed(bounds.minX(), bounds.maxX()))
                .boxed()
                .flatMapToLong(x -> (parallel ? IntStream.rangeClosed(bounds.minY(), bounds.maxY()).parallel() : IntStream.rangeClosed(bounds.minY(), bounds.maxY()))
                        .mapToLong(y -> xy2tilePos(x, y)));
    }
}
