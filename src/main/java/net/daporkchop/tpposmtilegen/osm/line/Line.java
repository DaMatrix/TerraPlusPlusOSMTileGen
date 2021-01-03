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

package net.daporkchop.tpposmtilegen.osm.line;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.osm.Geometry;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.Point;

import java.util.Map;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
public final class Line implements Geometry {
    protected final long id;
    protected final Map<String, String> tags;
    protected final Point[] points;

    public Line(long id, @NonNull Map<String, String> tags, @NonNull Point[] points) {
        this.id = notNegative(id, "id");
        this.tags = tags;
        checkArg(points.length >= 2, "line must consist of at least 2 points!");
        this.points = points;
    }

    @Override
    public void _toGeoJSON(StringBuilder dst) {
        dst.append("{\"type\":\"LineString\",\"coordinates\":[");
        for (Point point : this.points) {
            dst.append('[');
            Point.appendCoordinate(point.x(), dst);
            dst.append(',');
            Point.appendCoordinate(point.y(), dst);
            dst.append(']').append(',');
        }
        dst.setCharAt(dst.length() - 1, ']');
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
}
