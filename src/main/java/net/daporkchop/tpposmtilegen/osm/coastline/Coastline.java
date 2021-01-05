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

package net.daporkchop.tpposmtilegen.osm.coastline;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.util.Collections;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
public final class Coastline extends Element {
    private static final Map<String, String> TAGS = Collections.singletonMap("natural", "coastline");

    public static final int TYPE = 3;

    public Coastline(long id) {
        super(id, TAGS);
    }

    public Coastline(long id, @NonNull ByteBuf data) {
        super(id, data);
    }

    @Override
    public int type() {
        return TYPE;
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
    }

    @Override
    protected void fromBytes(@NonNull ByteBuf src) {
    }

    @Override
    public void computeReferences(@NonNull Storage storage) throws Exception {
        //coastline isn't a real OpenStreetMap primitive type, so we skip it
    }

    @Override
    public Geometry toGeometry(@NonNull Storage storage) throws Exception {
        return null;
    }
}
