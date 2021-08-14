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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
public class Area implements Geometry {
    protected final Shape[] shapes;

    public Area(@NonNull Shape[] shapes) {
        notNegative(shapes.length, "area must consist of at least one shape!");
        this.shapes = shapes;
    }

    public Area(@NonNull ByteBuf src) {
        this.shapes = new Shape[src.readIntLE()];
        for (int i = 0; i < this.shapes.length; i++) {
            this.shapes[i] = new Shape(src);
        }
    }

    @Override
    public void toGeoJSON(@NonNull StringBuilder dst) {
        if (this.shapes.length == 1) {
            dst.append("{\"type\":\"Polygon\",\"coordinates\":");
            Shape.emitPolygon(this.shapes[0], dst);
        } else {
            dst.append("{\"type\":\"MultiPolygon\",\"coordinates\":[");
            for (Shape shape : this.shapes) {
                Shape.emitPolygon(shape, dst);
                dst.append(',');
            }
            dst.setCharAt(dst.length() - 1, ']');
        }
        dst.append('}');
    }

    @Override
    public long[] listIntersectedTiles() {
        if (this.shapes.length == 1) { //only a single shape, so we don't need to aggregate intersected tiles from multiple child shapes
            return this.shapes[0].listIntersectedTiles();
        } else {
            LongSet tilePositions = new LongOpenHashSet();
            for (Shape shape : this.shapes) {
                tilePositions.addAll(LongArrayList.wrap(shape.listIntersectedTiles()));
            }
            return tilePositions.toLongArray();
        }
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeIntLE(this.shapes.length);
        for (Shape shape : this.shapes) {
            shape.toBytes(dst);
        }
    }

    @Override
    public Area simplify(double targetPointDensity) {
        //simplify each shape individually, discarding all shapes that discarded themselves and discarding ourself if no shapes remain
        List<Shape> simplifiedShapes = new ArrayList<>(this.shapes.length);
        for (Shape shape : this.shapes) {
            Shape simplifiedShape = shape.simplify(targetPointDensity);
            if (simplifiedShape != null) {
                simplifiedShapes.add(simplifiedShape);
            }
        }

        return simplifiedShapes.isEmpty() ? null : new Area(simplifiedShapes.toArray(new Shape[0]));
    }

    @Override
    public WeightedDouble averagePointDensity() {
        return Stream.of(this.shapes).map(Shape::averagePointDensity).reduce(WeightedDouble::add).get();
    }
}
