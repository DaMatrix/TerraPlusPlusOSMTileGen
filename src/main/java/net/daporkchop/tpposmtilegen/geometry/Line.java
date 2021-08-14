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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;

import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.util.Arrays;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
@Getter
@ToString
public final class Line extends ComplexGeometry {
    protected static void emitLineString(Point[] points, StringBuilder dst) {
        dst.append('[');
        for (Point point : points) {
            dst.append('[');
            Point.appendCoordinate(point.x(), dst);
            dst.append(',');
            Point.appendCoordinate(point.y(), dst);
            dst.append(']').append(',');
        }
        dst.setCharAt(dst.length() - 1, ']');
    }

    protected final Point[] points;

    public Line(@NonNull Point[] points) {
        checkArg(points.length >= 2, "line must consist of at least 2 points!");
        this.points = points;
    }

    public Line(@NonNull ByteBuf src) {
        this.points = new Point[src.readIntLE()];
        for (int i = 0; i < this.points.length; i++) {
            this.points[i] = new Point(src);
        }
    }

    @Override
    public void toGeoJSON(@NonNull StringBuilder dst) {
        dst.append("{\"type\":\"LineString\",\"coordinates\":");
        emitLineString(this.points, dst);
        dst.append('}');
    }

    @Override
    public Bounds2d computeObjectBounds() {
        int minX = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (Point point : this.points) {
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
        long[] arr = new long[tileCount];
        int i = 0;

        Line2D.Double line = new Line2D.Double();
        Rectangle2D.Double rectangle = new Rectangle2D.Double();

        for (int x = tileMinX; x <= tileMaxX; x++) { //iterate through every tile
            for (int y = tileMinY; y <= tileMaxY; y++) {
                rectangle.setRect(tile2point(x), tile2point(y), TILE_SIZE_POINT_SCALE, TILE_SIZE_POINT_SCALE);

                Point prevPoint = this.points[0];
                for (int p = 1, len = this.points.length; p < len; p++) { //iterate through every line segment
                    Point currPoint = this.points[p];
                    line.setLine(prevPoint.x(), prevPoint.y(), currPoint.x(), currPoint.y());

                    if (line.intersects(rectangle)) {
                        arr[i++] = xy2tilePos(x, y);
                        break; //don't need to check any other segments
                    }
                }
            }
        }

        return i == tileCount ? arr : Arrays.copyOf(arr, i);
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        int count = this.points.length;
        dst.writeIntLE(count);
        for (int i = 0; i < count; i++) {
            this.points[i].toBytes(dst);
        }
    }

    @Override
    public Line simplify(double targetPointDensity) {
        //simplify the single line string, discarding ourself if needed
        Point[] simplifiedPoints = simplifyPointString(this.points, targetPointDensity, false);
        return simplifiedPoints != null ? new Line(simplifiedPoints) : null;
    }

    @Override
    public WeightedDouble averagePointDensity() {
        double sum = 0.0d;
        long cnt = 0L;

        int count = this.points.length;
        for (int i = 1; i < count; i++) {
            Point p0 = this.points[i - 1];
            Point p1 = this.points[i];
            long dx = p0.x() - p1.x();
            long dy = p0.y() - p1.y();
            if ((dx | dy) != 0) {
                sum += sqrt(dx * dx + dy * dy);
                cnt++;
            }
        }

        return new WeightedDouble(sum, cnt);
    }
}
