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

package net.daporkchop.tpposmtilegen.geojson.geometry;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
class GeometryDeserializer extends JsonDeserializer<Geometry> {
    private static final Map<String, Class<? extends Geometry>> TYPE_CLASSES_BY_NAME = new HashMap<>();

    static {
        TYPE_CLASSES_BY_NAME.put("GeometryCollection", GeometryCollection.class);
        TYPE_CLASSES_BY_NAME.put("LineString", LineString.class);
        TYPE_CLASSES_BY_NAME.put("MultiLineString", MultiLineString.class);
        TYPE_CLASSES_BY_NAME.put("MultiPoint", MultiPoint.class);
        TYPE_CLASSES_BY_NAME.put("MultiPolygon", MultiPolygon.class);
        TYPE_CLASSES_BY_NAME.put("Point", Point.class);
        TYPE_CLASSES_BY_NAME.put("Polygon", Polygon.class);
    }

    @Override
    public Geometry deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String nextName = p.nextFieldName();
        checkState("type".equals(nextName), "first field must be \"type\", not \"%s\"", nextName);

        String typeName = p.nextTextValue();
        Class<? extends Geometry> typeClass = TYPE_CLASSES_BY_NAME.get(typeName);
        checkState(typeClass != null, "unknown GeoJSON type: \"%s\"!", typeName);

        p.nextToken();
        return ctxt.readValue(p, typeClass);
    }
}
