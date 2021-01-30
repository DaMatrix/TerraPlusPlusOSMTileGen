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

package net.daporkchop.tpposmtilegen.geometry;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.Bounds2d;

import java.awt.Polygon;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * A closed line loop in an {@link Area}, possibly with holes.
 * <p>
 * Should probably be named {@code Polygon}, but I already have a class with that name...
 *
 * @author DaPorkchop_
 */
@ToString
public final class Shape extends ComplexGeometry {
    private static Point[] loopFromBytes(ByteBuf src) {
        int count = src.readIntLE();
        Point[] loop = new Point[count + 1];
        for (int i = 0; i < count; i++) {
            loop[i] = new Point(src);
        }
        loop[count] = loop[0];
        return loop;
    }

    protected static void emitPolygon(Shape shape, StringBuilder dst) {
        dst.append('[');
        Line.emitLineString(shape.outerLoop, dst);
        for (Point[] innerLoop : shape.innerLoops) {
            dst.append(',');
            Line.emitLineString(innerLoop, dst);
        }
        dst.append(']');
    }

    protected final Point[] outerLoop;
    protected final Point[][] innerLoops;

    public Shape(@NonNull Point[] outerLoop, @NonNull Point[][] innerLoops) {
        checkArg(outerLoop.length >= 3, "outerLoop must contain at least 3 points! (found: %d)", outerLoop.length);
        for (int i = 0; i < innerLoops.length; i++) {
            Point[] innerLoop = innerLoops[i];
            checkArg(innerLoop != null, "innerLoop[%d] is null!", i);
            checkArg(innerLoop.length >= 3, "innerLoop[%d] must contain at least 3 points! (found: %d)", i, innerLoop.length);
        }

        this.outerLoop = outerLoop;
        this.innerLoops = innerLoops;
    }

    public Shape(@NonNull ByteBuf src) {
        this.outerLoop = loopFromBytes(src);
        this.innerLoops = new Point[src.readIntLE()][];
        for (int i = 0; i < this.innerLoops.length; i++) {
            this.innerLoops[i] = loopFromBytes(src);
        }
    }

    @Override
    public Bounds2d computeObjectBounds() {
        int minX = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (Point point : this.outerLoop) {
            int x = point.x();
            int y = point.y();
            minX = min(minX, x);
            maxX = max(maxX, x);
            minY = min(minY, y);
            maxY = max(maxY, y);
        }
        return Bounds2d.of(minX, maxX, minY, maxY);
    }

    @Override
    protected long[] listIntersectedTilesComplex(int tileMinX, int tileMaxX, int tileMinY, int tileMaxY, int tileCount) {
        //convert all loops into java.awt.Polygons
        PolygonRecycler recycler = POLYGON_RECYCLER.get();
        Polygon outerPoly = recycler.fromClosedLineString(this.outerLoop);
        int innerCount = this.innerLoops.length;
        Polygon[] innerPolys = new Polygon[innerCount];
        for (int i = 0; i < innerCount; i++) {
            innerPolys[i] = recycler.fromClosedLineString(this.innerLoops[i]);
        }

        try {
            LongSet tilePositions = new LongOpenHashSet(tileCount);

            for (int x = tileMinX; x <= tileMaxX; x++) {
                for (int y = tileMinY; y <= tileMaxY; y++) {
                    ADD:
                    if (outerPoly.intersects(tile2point(x), tile2point(y), TILE_SIZE_POINT_SCALE, TILE_SIZE_POINT_SCALE)) {
                        for (Polygon innerPoly : innerPolys) {
                            if (innerPoly.contains(tile2point(x), tile2point(y), TILE_SIZE_POINT_SCALE, TILE_SIZE_POINT_SCALE)) { //tile is entirely contained within a hole
                                break ADD;
                            }
                        }
                        tilePositions.add(xy2tilePos(x, y));
                    }
                }
            }
            return tilePositions.toLongArray();
        } finally {
            for (int i = innerCount - 1; i >= 0; i--) {
                recycler.release(innerPolys[i]);
            }
            recycler.release(outerPoly);
        }
    }

    @Override
    public void toGeoJSON(@NonNull StringBuilder dst) {
        dst.append("{\"type\":\"Polygon\",\"coordinates\":");
        emitPolygon(this, dst);
        dst.append('}');
    }

    @Override
    public boolean shouldStoreExternally(int tiles, int dataSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        this.loopToBytes(dst, this.outerLoop);
        dst.writeIntLE(this.innerLoops.length);
        for (Point[] loop : this.innerLoops) {
            this.loopToBytes(dst, loop);
        }
    }

    private void loopToBytes(ByteBuf dst, Point[] loop) {
        int count = loop.length - 1;
        dst.writeIntLE(count);
        for (int i = 0; i < count; i++) {
            loop[i].toBytes(dst);
        }
    }
}
