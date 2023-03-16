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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
@Getter
@ToString
public final class MultiPoint implements Geometry {
    static void emitMultiPointCoordinates(Point[] points, StringBuilder dst) {
        if (points.length == 0) {
            dst.append("[]");
            return;
        }

        dst.append('[');
        points[0].emitPointCoordinates(dst);
        for (int i = 1; i < points.length; i++) {
            dst.append(',');
            points[i].emitPointCoordinates(dst);
        }
        dst.append(']');
    }

    static Bounds2d computeBounds(Point[] points) {
        int minX = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (Point point : points) {
            int x = point.x();
            int y = point.y();
            minX = min(minX, x);
            maxX = max(maxX, x);
            minY = min(minY, y);
            maxY = max(maxY, y);
        }
        return Bounds2d.of(minX, maxX, minY, maxY);
    }

    protected final Point[] points;

    public MultiPoint(@NonNull Point[] points) {
        checkArg(points.length >= 1, "MultiPoint must consist of at least 1 point!");
        this.points = points;
    }

    public MultiPoint(@NonNull ByteBuf src) {
        this.points = new Point[src.readIntLE()];
        for (int i = 0; i < this.points.length; i++) {
            this.points[i] = new Point(src);
        }
    }

    @Override
    public long[] listIntersectedTiles(int level) {
        if (this.points.length == 1) { //special case if there's only one point
            Point point = this.points[0];
            return new long[]{ point2tile(level, point.x(), point.y()) };
        } else {
            LongSet set = new LongOpenHashSet();
            for (Point point : this.points) {
                set.add(point2tile(level, point.x(), point.y()));
            }
            return set.toLongArray();
        }
    }

    @Override
    public void toGeoJSON(@NonNull StringBuilder dst) {
        if (this.points.length == 1) { //special case if there's only one point
            this.points[0].toGeoJSON(dst);
        } else {
            dst.append("{\"type\":\"MultiPoint\",\"coordinates\":");
            emitMultiPointCoordinates(this.points, dst);
            dst.append('}');
        }
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeIntLE(this.points.length);
        for (Point point : this.points) {
            point.toBytes(dst);
        }
    }

    @Override
    public Optional<? extends Geometry> simplifyTo(int targetLevel) {
        Point[] simplifiedPoints = Stream.of(this.points)
                .map(point -> point.simplifyTo(targetLevel).orElse(null))
                .filter(Objects::nonNull)
                .toArray(Point[]::new);

        switch (simplifiedPoints.length) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(simplifiedPoints[0]);
            default:
                return Optional.of(new MultiPoint(simplifiedPoints));
        }
    }

    @Override
    public Bounds2d bounds() {
        return computeBounds(this.points);
    }

    @Override
    public WeightedDouble averagePointDensity() {
        throw new UnsupportedOperationException();
    }
}
