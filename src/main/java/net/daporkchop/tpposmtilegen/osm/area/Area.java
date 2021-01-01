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

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;

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
public final class Area {
    public static long elementIdToAreaId(@NonNull Element element) {
        switch (element.type()) {
            case Way.TYPE:
                return element.id() << 1L;
            case Relation.TYPE:
                return (element.id() << 1L) | 1L;
        }
        throw new IllegalArgumentException("element cannot be converted to area: " + element);
    }

    protected final long id;
    protected final Shape[] shapes;

    public Area(long id, @NonNull Shape[] shapes) {
        this.id = notNegative(id, "id");
        notNegative(shapes.length, "area must consist of at least one shape!");
        this.shapes = shapes;
    }
}
