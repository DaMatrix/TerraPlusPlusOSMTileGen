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

package net.daporkchop.tpposmtilegen.geometry;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;

import java.awt.Polygon;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountedCompleter;
import java.util.stream.IntStream;

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
        checkArg(outerLoop.length >= 4, "outerLoop must contain at least 4 points! (found: %d)", outerLoop.length);
        checkArg(outerLoop[0].equals(outerLoop[outerLoop.length - 1]), "outerLoop must be a closed loop!");
        for (int i = 0; i < innerLoops.length; i++) {
            Point[] innerLoop = innerLoops[i];
            checkArg(innerLoop != null, "innerLoop[%d] is null!", i);
            checkArg(innerLoop.length >= 4, "innerLoop[%d] must contain at least 4 points! (found: %d)", i, innerLoop.length);
            checkArg(innerLoop[0].equals(innerLoop[innerLoop.length - 1]), "innerLoop[%d] must be a closed loop!", i);
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
    public Bounds2d bounds() {
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
    protected long[] listIntersectedTilesComplex(int level, int tileMinX, int tileMaxX, int tileMinY, int tileMaxY, int tileCount) {
        //convert all loops into java.awt.Polygons
        PolygonRecycler recycler = POLYGON_RECYCLER.get();
        Polygon outerPoly = recycler.fromClosedLineString(this.outerLoop);
        int innerCount = this.innerLoops.length;
        Polygon[] innerPolys = new Polygon[innerCount];
        for (int i = 0; i < innerCount; i++) {
            innerPolys[i] = recycler.fromClosedLineString(this.innerLoops[i]);
        }

        try {
            LongList tilePositions = new LongArrayList(tileCount);
            listIntersectedTilesComplex(tilePositions, outerPoly, innerPolys, level, tileMinX, tileMinY, tileMaxX - tileMinX + 1, tileMaxY - tileMinY + 1);
            return tilePositions.toLongArray();
        } finally {
            for (int i = innerCount - 1; i >= 0; i--) {
                recycler.release(innerPolys[i]);
            }
            recycler.release(outerPoly);
        }
    }

    private static void listIntersectedTilesComplex(LongList tilePositions, Polygon outerPoly, Polygon[] innerPolys, int level, int baseTileX, int baseTileY, int sizeX, int sizeY) {
        positive(sizeX, "sizeX");
        positive(sizeY, "sizeY");

        int tileSizePointScale = tileSizePointScale(level);

        int pointsBaseX = tile2point(level, baseTileX);
        int pointsBaseY = tile2point(level, baseTileY);
        int pointsSizeX = multiplyExact(tileSizePointScale, sizeX);
        int pointsSizeY = multiplyExact(tileSizePointScale, sizeY);

        if (outerPoly.intersects(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) {
            for (Polygon innerPoly : innerPolys) {
                if (innerPoly.contains(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) { //tile is entirely contained within a hole
                    return;
                }
            }

            if (sizeX == 1 && sizeY == 1) { //we can't recurse any further, add the tile to the output list and stop
                tilePositions.add(xy2tilePos(baseTileX, baseTileY));
                return;
            }

            CONTAINED:
            if (outerPoly.contains(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) {
                for (Polygon innerPoly : innerPolys) {
                    if (innerPoly.intersects(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) { //tile intersects a hole, so we might not entirely contain the whole thing
                        break CONTAINED;
                    }
                }

                //we're entirely contained by the polygon, so we can immediately add every tile!
                for (int dx = 0; dx < sizeX; dx++) {
                    for (int dy = 0; dy < sizeY; dy++) {
                        tilePositions.add(xy2tilePos(baseTileX + dx, baseTileY + dy));
                    }
                }
                return;
            }

            //we intersect the polygon partially, recurse into each of the 4 children and try again
            for (int dx = 0; dx <= 1; dx++) {
                for (int dy = 0; dy <= 1; dy++) {
                    int subBaseX = baseTileX + ((sizeX >> 1) & -dx);
                    int subBaseY = baseTileY + ((sizeY >> 1) & -dy);
                    int subSizeX = (sizeX >> 1) + (sizeX & dx);
                    int subSizeY = (sizeY >> 1) + (sizeY & dy);

                    if (subSizeX != 0 && subSizeY != 0) {
                        listIntersectedTilesComplex(tilePositions, outerPoly, innerPolys, level, subBaseX, subBaseY, subSizeX, subSizeY);
                    }
                }
            }
        }
    }

    //TODO: determine whether this would be beneficial
    private static class ListIntersectedTilesComplexTask extends CountedCompleter<Void> {
        final LongList tilePositions;
        final Polygon outerPoly;
        final Polygon[] innerPolys;
        final int level;
        final int baseTileX;
        final int baseTileY;
        final int sizeX;
        final int sizeY;

        protected ListIntersectedTilesComplexTask(CountedCompleter<?> completer, LongList tilePositions, Polygon outerPoly, Polygon[] innerPolys, int level, int baseTileX, int baseTileY, int sizeX, int sizeY) {
            super(completer, multiplyExact(positive(sizeX, "sizeX"), positive(sizeY, "sizeY")));

            this.tilePositions = tilePositions;
            this.outerPoly = outerPoly;
            this.innerPolys = innerPolys;
            this.level = level;
            this.baseTileX = baseTileX;
            this.baseTileY = baseTileY;
            this.sizeX = sizeX;
            this.sizeY = sizeY;
        }

        @Override
        public void compute() {
            int tileSizePointScale = tileSizePointScale(this.level);

            int pointsBaseX = tile2point(this.level, this.baseTileX);
            int pointsBaseY = tile2point(this.level, this.baseTileY);
            int pointsSizeX = multiplyExact(tileSizePointScale, this.sizeX);
            int pointsSizeY = multiplyExact(tileSizePointScale, this.sizeY);

            if (this.outerPoly.intersects(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) {
                for (Polygon innerPoly : this.innerPolys) {
                    if (innerPoly.contains(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) { //tile is entirely contained within a hole
                        this.tryComplete();
                        return;
                    }
                }

                if (this.sizeX == 1 && this.sizeY == 1) { //we can't recurse any further, add the tile to the output list and stop
                    this.tilePositions.add(xy2tilePos(this.baseTileX, this.baseTileY));
                    this.tryComplete();
                    return;
                }

                CONTAINED:
                if (this.outerPoly.contains(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) {
                    for (Polygon innerPoly : this.innerPolys) {
                        if (innerPoly.intersects(pointsBaseX, pointsBaseY, pointsSizeX, pointsSizeY)) { //tile intersects a hole, so we might not entirely contain the whole thing
                            break CONTAINED;
                        }
                    }

                    //we're entirely contained by the polygon, so we can immediately add every tile!
                    for (int dx = 0; dx < this.sizeX; dx++) {
                        for (int dy = 0; dy < this.sizeY; dy++) {
                            this.tilePositions.add(xy2tilePos(this.baseTileX + dx, this.baseTileY + dy));
                        }
                    }
                    this.tryComplete();
                    return;
                }

                //we intersect the polygon partially, recurse into each of the 4 children and try again
                if (this.sizeX == 1) {
                    this.setPendingCount(2);
                    for (int dy = 0; dy <= 1; dy++) {
                        int subBaseY = this.baseTileY + ((this.sizeY >> 1) & -dy);
                        int subSizeY = (this.sizeY >> 1) + (this.sizeY & dy);
                        checkState(subSizeY > 0, subSizeY);

                        new ListIntersectedTilesComplexTask(this, this.tilePositions, this.outerPoly, this.innerPolys, this.level, this.baseTileX, subBaseY, this.sizeX, subSizeY).fork();
                    }
                } else if (this.sizeY == 1) {
                    this.setPendingCount(2);
                    for (int dx = 0; dx <= 1; dx++) {
                        int subBaseX = this.baseTileX + ((this.sizeX >> 1) & -dx);
                        int subSizeX = (this.sizeX >> 1) + (this.sizeX & dx);
                        checkState(subSizeX > 0, subSizeX);

                        new ListIntersectedTilesComplexTask(this, this.tilePositions, this.outerPoly, this.innerPolys, this.level, subBaseX, this.baseTileY, subSizeX, this.sizeY).fork();
                    }
                } else {
                    this.setPendingCount(4);
                    for (int dx = 0; dx <= 1; dx++) {
                        for (int dy = 0; dy <= 1; dy++) {
                            int subBaseX = this.baseTileX + ((this.sizeX >> 1) & -dx);
                            int subBaseY = this.baseTileY + ((this.sizeY >> 1) & -dy);
                            int subSizeX = (this.sizeX >> 1) + (this.sizeX & dx);
                            int subSizeY = (this.sizeY >> 1) + (this.sizeY & dy);
                            checkState(subSizeY > 0, subSizeY);
                            checkState(subSizeX > 0, subSizeX);

                            new ListIntersectedTilesComplexTask(this, this.tilePositions, this.outerPoly, this.innerPolys, this.level, subBaseX, subBaseY, subSizeX, subSizeY).fork();
                        }
                    }
                }
            }
            this.tryComplete();
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

    @Override
    public Shape simplifyTo(int targetLevel) {
        if (targetLevel == 0) {
            return this;
        }

        //simplify each loop individually, discarding the whole shape if the outer loop is discarded and silently discarding inner loops as needed
        Point[] simplifiedOuterLoop = simplifyVisvalingamWhyatt(this.outerLoop, targetLevel, true);
        if (simplifiedOuterLoop == null) {
            return null;
        }

        List<Point[]> simplifiedInnerLoops = new ArrayList<>(this.innerLoops.length);
        for (Point[] innerLoop : this.innerLoops) {
            Point[] simplifiedInnerLoop = simplifyVisvalingamWhyatt(innerLoop, targetLevel, true);
            if (simplifiedInnerLoop != null) {
                simplifiedInnerLoops.add(simplifiedInnerLoop);
            }
        }

        return new Shape(simplifiedOuterLoop, simplifiedInnerLoops.toArray(new Point[0][]));
    }

    @Override
    public WeightedDouble averagePointDensity() {
        double sum = 0.0d;
        long cnt = 0L;

        for (int i = 1; i < this.outerLoop.length; i++) {
            Point p0 = this.outerLoop[i - 1];
            Point p1 = this.outerLoop[i];
            long dx = p0.x() - p1.x();
            long dy = p0.y() - p1.y();
            if ((dx | dy) != 0) {
                sum += sqrt(dx * dx + dy * dy);
                cnt++;
            }
        }
        for (Point[] loop : this.innerLoops) {
            for (int i = 1; i < loop.length; i++) {
                Point p0 = loop[i - 1];
                Point p1 = loop[i];
                long dx = p0.x() - p1.x();
                long dy = p0.y() - p1.y();
                if ((dx | dy) != 0) {
                    sum += sqrt(dx * dx + dy * dy);
                    cnt++;
                }
            }
        }

        return new WeightedDouble(sum, cnt);
    }
}
