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

package net.daporkchop.tpposmtilegen.util;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import lombok.NonNull;

import java.util.Map;

/**
 * @author DaPorkchop_
 */
public interface ToGeoJSONSerializable {
    Map<String, String> tags();

    default void toGeoJSON(@NonNull StringBuilder dst) {
        Map<String, String> tags = this.tags();
        boolean tagsEmpty = tags.isEmpty();
        if (!tagsEmpty) {
            dst.append("{\"type\":\"Feature\",\"geometry\":");
        }
        this._toGeoJSON(dst);
        if (!tagsEmpty) {
            dst.append(",\"properties\":{");
            tags.forEach((k, v) -> {
                dst.append('"');
                JsonStringEncoder.getInstance().quoteAsString(k, dst);
                dst.append('"').append(':').append('"');
                JsonStringEncoder.getInstance().quoteAsString(v, dst);
                dst.append('"').append(',');
            });
            dst.setCharAt(dst.length() - 1, '}');
            dst.append('}');
        }
        dst.append('\n');
    }

    void _toGeoJSON(StringBuilder dst);
}
