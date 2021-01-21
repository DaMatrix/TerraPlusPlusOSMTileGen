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

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.util.Persistent;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
public interface Geometry extends Persistent {
    static void toGeoJSON(@NonNull StringBuilder dst, @NonNull Geometry geometry, @NonNull Map<String, String> tags, long combinedId) {
        dst.append("{\"type\":\"Feature\",\"geometry\":");

        //geometry
        geometry.toGeoJSON(dst);

        //tags
        if (!tags.isEmpty()) {
            dst.append(",\"properties\":{");
            tags.forEach((k, v) -> {
                dst.append('"');
                JsonStringEncoder.getInstance().quoteAsString(k, dst);
                dst.append('"').append(':').append('"');
                JsonStringEncoder.getInstance().quoteAsString(v, dst);
                dst.append('"').append(',');
            });
            dst.setCharAt(dst.length() - 1, '}');
        }

        //id
        dst.append(",\"id\":\"")
                .append(Element.typeName(Element.extractType(combinedId)))
                .append('/')
                .append(Element.extractId(combinedId)).append('"');

        dst.append('}').append('\n');
    }

    static ByteBuffer toBytes(@NonNull CharSequence text) { //really should go in a separate util class but i don't want to make one
        int length = text.length();
        ByteBuffer buffer = ByteBuffer.allocateDirect(length);
        for (int i = 0; i < length; i++) {
            char c = text.charAt(i);
            buffer.put((byte) (c > 255 ? '?' : c));
        }
        buffer.flip();
        return buffer;
    }

    long[] listIntersectedTiles();

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
               && dataSize > 2048;
    }

    default String externalStorageLocation(int type, long id) {
        return PStrings.fastFormat("%s/%03d/%03d/%d.json", Element.typeName(type), id % 1000L, (id / 1000L) % 1000L, id / 1000_000L);
    }

    void toGeoJSON(@NonNull StringBuilder dst);
}
