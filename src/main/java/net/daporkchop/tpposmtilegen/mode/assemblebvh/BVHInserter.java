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

package net.daporkchop.tpposmtilegen.mode.assemblebvh;

import lombok.NonNull;
import net.daporkchop.tpposmtilegen.geojson.GeoJSONObject;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapBVH;

import java.io.File;
import java.io.IOException;

/**
 * @author DaPorkchop_
 */
public class BVHInserter implements PipelineStep<GeoJSONObject> {
    protected final OffHeapBVH bvh;

    public BVHInserter(@NonNull File dstFile) throws IOException {
        this.bvh = new OffHeapBVH(dstFile.toPath(), Bounds2d.of(-180.0d, 180.0d, -90.0d, 90.0d),
                1L << 40L); //1 TiB
    }

    @Override
    public void accept(@NonNull GeoJSONObject value) throws IOException {
    }

    @Override
    public void close() throws IOException {
        this.bvh.close();
    }
}
