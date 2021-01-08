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

import lombok.NonNull;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;

import java.awt.Polygon;

import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
public abstract class ComplexGeometry implements Geometry {
    protected static final Ref<PolygonRecycler> POLYGON_RECYCLER = ThreadRef.soft(PolygonRecycler::new);

    public abstract Bounds2d computeObjectBounds();

    @Override
    public final long[] listIntersectedTiles() {
        Bounds2d bounds = this.computeObjectBounds();
        int tileMinX = point2tile(bounds.minX());
        int tileMaxX = point2tile(bounds.maxX());
        int tileMinY = point2tile(bounds.minY());
        int tileMaxY = point2tile(bounds.maxY());
        int tileCount = (tileMaxX - tileMinX + 1) * (tileMaxY - tileMinY + 1);

        if (tileCount == 0) { //wtf?!?
            throw new IllegalStateException();
        } else if (tileCount == 1) { //only intersects a single tile
            return new long[]{ xy2tilePos(tileMinX, tileMinY) };
        } else if (tileMinX == tileMaxX || tileMinY == tileMaxY) { //only intersects a single row/column of tiles
            long[] arr = new long[tileCount];
            for (int i = 0, x = tileMinX; x <= tileMaxX; x++) {
                for (int y = tileMinY; y <= tileMaxY; y++) {
                    arr[i++] = xy2tilePos(x, y);
                }
            }
            return arr;
        } else {
            return this.listIntersectedTilesComplex(tileMinX, tileMaxX, tileMinY, tileMaxY, tileCount);
        }
    }

    protected long[] listIntersectedTilesComplex(int tileMinX, int tileMaxX, int tileMinY, int tileMaxY, int tileCount) {
        long[] arr = new long[tileCount];
        for (int i = 0, x = tileMinX; x <= tileMaxX; x++) {
            for (int y = tileMinY; y <= tileMaxY; y++) {
                arr[i++] = xy2tilePos(x, y);
            }
        }
        return arr;
    }

    protected static final class PolygonRecycler extends SimpleRecycler<Polygon> {
        public Polygon fromClosedLineString(@NonNull Point[] points) {
            Polygon polygon = this.get();

            for (int i = 0, lim = points.length - 1; i < lim; i++) { //skip last point because polygon is closed
                polygon.addPoint(points[i].x(), points[i].y());
            }
            return polygon;
        }

        @Override
        protected Polygon newInstance0() {
            return new Polygon();
        }

        @Override
        protected void reset0(@NonNull Polygon value) {
            value.reset();
        }

        @Override
        protected boolean hasCapacity() {
            return this.size() < 8; //don't store more than 8 polygon instances
        }
    }
}
