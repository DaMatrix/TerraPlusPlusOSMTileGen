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

import io.netty.util.concurrent.FastThreadLocal;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.geojson.GeoJSONObject;
import net.daporkchop.tpposmtilegen.geojson.feature.Feature;
import net.daporkchop.tpposmtilegen.geojson.feature.FeatureCollection;
import net.daporkchop.tpposmtilegen.geojson.geometry.Point;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.storage.Node;
import net.daporkchop.tpposmtilegen.storage.NodeDB;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author DaPorkchop_
 */
public class IndexBuilder implements PipelineStep<GeoJSONObject> {
    protected final FastThreadLocal<NodeDB> threadLocalStorage;
    protected final AtomicLong counter = new AtomicLong();

    public IndexBuilder(@NonNull File root) throws IOException {
        this.threadLocalStorage = NodeDB.threadLocalStorage(root);
    }

    @Override
    public void accept(@NonNull GeoJSONObject value) throws IOException {
        Map<String, String> tags = Collections.emptyMap();

        if (value instanceof FeatureCollection) {
            for (Feature feature : ((FeatureCollection) value).features()) {
                this.accept(feature);
            }
            return;
        } else if (value instanceof Feature) {
            tags = ((Feature) value).properties();
            value = ((Feature) value).geometry();
        }

        if (value instanceof Point) {
            Point point = (Point) value;

            try {
                this.threadLocalStorage.get()
                        .createNode(new Node(this.counter.getAndIncrement(), tags, point.lon(), point.lat()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
    }
}
