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

package net.daporkchop.tpposmtilegen.osm;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.util.Map;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString(callSuper = true)
public final class Node extends Element {
    public static final int TYPE = 0;

    public Node(long id, Map<String, String> tags) {
        super(id, tags);
    }

    public Node(long id, ByteBuf data) {
        super(id, data);
    }

    public Node tags(@NonNull Map<String, String> tags) {
        super.tags = tags;
        return this;
    }

    @Override
    public int type() {
        return TYPE;
    }

    @Override
    public void computeReferences(@NonNull Storage storage) throws Exception {
        //a node doesn't reference anything
    }

    @Override
    public Geometry toGeometry(@NonNull Storage storage) throws Exception {
        return storage.points().get(this.id);
    }
}
