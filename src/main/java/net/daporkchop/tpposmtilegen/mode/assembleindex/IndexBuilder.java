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

package net.daporkchop.tpposmtilegen.mode.assembleindex;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.sqlite.support.SQLiteIndexFlags;
import com.googlecode.cqengine.query.option.QueryOptions;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.geojson.GeoJSONObject;
import net.daporkchop.tpposmtilegen.geojson.feature.Feature;
import net.daporkchop.tpposmtilegen.geojson.feature.FeatureCollection;
import net.daporkchop.tpposmtilegen.geojson.geometry.LineString;
import net.daporkchop.tpposmtilegen.geojson.geometry.MultiPolygon;
import net.daporkchop.tpposmtilegen.geojson.geometry.Point;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.storage.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.*;

/**
 * @author DaPorkchop_
 */
public class IndexBuilder implements PipelineStep<GeoJSONObject> {
    protected final FastThreadLocal<Storage> threadLocalStorage;
    protected final AtomicLong counter = new AtomicLong();

    public IndexBuilder(@NonNull File root) throws IOException {
        this.threadLocalStorage = Storage.threadLocalStorage(root);
    }

    @Override
    public void accept(@NonNull GeoJSONObject value) throws IOException {
        Map<String, String> tags = null;

        if (value instanceof FeatureCollection) {
            for (Feature feature : ((FeatureCollection) value).features()) {
                this.accept(feature);
            }
            return;
        } else if (value instanceof Feature) {
            tags = ((Feature) value).properties();
            value = ((Feature) value).geometry();
        }

        Point[] points = null;
        if (value instanceof LineString) {
            points = ((LineString) value).points();
        } else if (value instanceof MultiPolygon) {
            points = ((MultiPolygon) value).polygons()[0].rings()[0].points();
        } else if (value instanceof Point) {
            points = new Point[]{ (Point) value };
        }

        if (points == null || points.length == 0) { //skip
            return;
        }

        Element element = new Element(this.counter.getAndIncrement());

        element.tags = tags;

        element.minX = Double.MAX_VALUE;
        element.maxX = Double.MIN_VALUE;
        element.minZ = Double.MAX_VALUE;
        element.maxZ = Double.MIN_VALUE;
        for (Point point : points) {
            element.minX = min(element.minX, point.lon());
            element.maxX = max(element.maxX, point.lon());
            element.minZ = min(element.minZ, point.lat());
            element.maxZ = max(element.maxZ, point.lat());
        }


    }

    @Override
    public void close() throws IOException {

    }
}
