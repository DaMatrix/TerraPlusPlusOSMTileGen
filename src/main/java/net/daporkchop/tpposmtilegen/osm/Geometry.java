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

package net.daporkchop.tpposmtilegen.osm;

import it.unimi.dsi.fastutil.longs.LongCollection;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.Persistent;
import net.daporkchop.tpposmtilegen.util.ToGeoJSONSerializable;

import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
public interface Geometry extends ToGeoJSONSerializable, Persistent {
    Bounds2d computeObjectBounds();

    default long[] listIntersectedTiles() {
        Bounds2d bounds = this.computeObjectBounds();
        int tileMinX = coord_point2tile(bounds.minX());
        int tileMaxX = coord_point2tile(bounds.maxX());
        int tileMinY = coord_point2tile(bounds.minY());
        int tileMaxY = coord_point2tile(bounds.maxY());
        long[] arr = new long[(tileMaxX - tileMinX + 1) * (tileMaxY - tileMinY + 1)];
        for (int i = 0, x = tileMinX; x <= tileMaxX; x++) {
            for (int y = tileMinY; y <= tileMaxY; y++) {
                arr[i++] = xy2tilePos(x, y);
            }
        }
        return arr;
    }

    /**
     * Checks whether or not this geometry object should be stored externally.
     * <p>
     * Externally stored objects will be written to tiles as a reference.
     *
     * @param tiles    the number of tiles that this geometry object intersects
     * @param dataSize the serialized size of this geometry object, in bytes
     * @return whether or not this geometry object should be stored externally
     */
    default boolean shouldStoreExternally(int tiles, int dataSize) {
        return tiles > 1 //if the object is only present in a single tile, there's obviously no reason to store this object as a
                && dataSize > 1024;
    }
}
