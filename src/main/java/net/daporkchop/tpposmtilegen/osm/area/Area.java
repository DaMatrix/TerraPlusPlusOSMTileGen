/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.osm.area;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.osm.Geometry;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.Persistent;
import net.daporkchop.tpposmtilegen.util.Point;

import java.util.Map;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * See:
 * <ul>
 *     <li><a href="https://wiki.openstreetmap.org/wiki/Area">wiki.openstreetmap.org/wiki/Area</a></li>
 *     <li><a href="https://wiki.openstreetmap.org/wiki/Relation:multipolygon">wiki.openstreetmap.org/wiki/Relation:multipolygon</a></li>
 * </ul>
 *
 * @author DaPorkchop_
 */
@Getter
@ToString
public final class Area implements Geometry {
    protected static void lineToGeoJSON(Point[] points, StringBuilder dst) {
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

    protected static void shapeToGeoJSON(Shape shape, StringBuilder dst) {
        dst.append('[');
        lineToGeoJSON(shape.outerLoop, dst);
        if (shape.innerLoops.length != 0) {
            for (Point[] innerLoop : shape.innerLoops) {
                dst.append(',');
                lineToGeoJSON(innerLoop, dst);
            }
        }
        dst.append(']');
    }

    protected final long gid;
    protected final Map<String, String> tags;
    protected final Shape[] shapes;

    public Area(long id, @NonNull Map<String, String> tags, @NonNull Shape[] shapes) {
        this.gid = notNegative(id, "id");
        this.tags = tags;
        notNegative(shapes.length, "area must consist of at least one shape!");
        this.shapes = shapes;
    }

    public Area(long id, @NonNull ByteBuf src) {
        this.gid = id;
        this.shapes = new Shape[src.readIntLE()];
        for (int i = 0; i < this.shapes.length; i++) {
            this.shapes[i] = new Shape(src);
        }
        this.tags = Persistent.readTags(src);
    }

    @Override
    public void _toGeoJSON(StringBuilder dst) {
        if (this.shapes.length == 1) {
            dst.append("{\"type\":\"Polygon\",\"coordinates\":");
            shapeToGeoJSON(this.shapes[0], dst);
        } else {
            dst.append("{\"type\":\"MultiPolygon\",\"coordinates\":[");
            for (Shape shape : this.shapes) {
                shapeToGeoJSON(shape, dst);
                dst.append(',');
            }
            dst.setCharAt(dst.length() - 1, ']');
        }
        dst.append('}');
    }

    @Override
    public Bounds2d computeObjectBounds() {
        int minX = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (Shape shape : this.shapes) {
            for (Point point : shape.outerLoop) {
                int x = point.x();
                int y = point.y();
                minX = min(minX, x);
                maxX = max(maxX, x);
                minY = min(minY, y);
                maxY = max(maxY, y);
            }
        }
        return Bounds2d.of(minX, maxX, minY, maxY);
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeIntLE(this.shapes.length);
        for (Shape shape : this.shapes) {
            shape.toBytes(dst);
        }
        Persistent.writeTags(dst, this.tags);
    }
}
