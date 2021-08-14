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

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
public abstract class ComplexGeometry implements Geometry {
    protected static final Ref<PolygonRecycler> POLYGON_RECYCLER = ThreadRef.soft(PolygonRecycler::new);

    protected static long sq(long l) {
        return l * l;
    }

    protected static Point[] simplifyPointString(@NonNull Point[] inPoints, double targetPointDensity, boolean closed) {
        double inLength = 0.0d;
        double inDensity = 0.0d;
        int inSegments = 0;

        for (int i = 1; i < inPoints.length; i++) {
            Point p0 = inPoints[i - 1];
            Point p1 = inPoints[i];
            long dx = p0.x() - p1.x();
            long dy = p0.y() - p1.y();
            if ((dx | dy) != 0) {
                inLength += sqrt(dx * dx + dy * dy);
                inSegments++;
            }
        }
        if (inSegments == 0) { //there are no points?!? bruh
            return null;
        }
        inDensity = inLength / inSegments;

        if (inDensity >= targetPointDensity) { //already sufficiently sparse
            return inPoints;
        }

        int outLength = ceilI(inLength / targetPointDensity);
        if (outLength < (closed ? 4 : 2)) { //too few points would be emitted, discard outself
            return null;
        }

        //generate exactly outLength points spaced exactly outDensity distance units apart on the original line
        double outDensity = inLength / outLength;
        Point[] outPoints = new Point[outLength];
        outPoints[0] = inPoints[0];

        Point inP0 = inPoints[0];
        Point inP1 = inPoints[1];
        double inTotalDist = sqrt(sq(inP0.x() - inP1.x()) + sq(inP0.y() - inP1.y()));
        double inRemainingDist = inTotalDist;
        int inIdx = 2;

        for (int outIdx = 1; outIdx < outLength; outIdx++) {
            double requestedDist = outDensity;

            double d = min(inRemainingDist, requestedDist);
            inRemainingDist -= d;
            requestedDist -= d;

            while (requestedDist > inRemainingDist) {
                inP0 = inP1;
                inP1 = inPoints[inIdx++];
                inRemainingDist = inTotalDist = sqrt(sq(inP0.x() - inP1.x()) + sq(inP0.y() - inP1.y()));

                d = min(inRemainingDist, requestedDist);
                inRemainingDist -= d;
                requestedDist -= d;
            }

            double f = 1.0d - inRemainingDist / inTotalDist;
            outPoints[outIdx] = new Point(lerpI(inP0.x(), inP1.x(), f), lerpI(inP0.y(), inP1.y(), f));
        }

        if (closed) { //make sure the loop is closed
            outPoints[outLength - 1] = outPoints[0];
        }

        return outPoints;
    }

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
