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
import net.daporkchop.tpposmtilegen.osm.area.Area;
import net.daporkchop.tpposmtilegen.osm.area.AreaKeys;
import net.daporkchop.tpposmtilegen.osm.area.Shape;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString(callSuper = true)
public final class Way extends Element {
    public static final int TYPE = 1;

    @NonNull
    protected long[] nodes;

    public Way(long id, Map<String, String> tags, @NonNull long[] nodes) {
        super(id, tags);

        this.nodes = nodes;
    }

    public Way(long id, ByteBuf data) {
        super(id, data);
    }

    public Way tags(@NonNull Map<String, String> tags) {
        super.tags = tags;
        return this;
    }

    @Override
    public int type() {
        return TYPE;
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeInt(this.nodes.length);
        for (long node : this.nodes) {
            dst.writeLong(node);
        }

        super.toBytes(dst);
    }

    @Override
    public void fromBytes(@NonNull ByteBuf src) {
        int count = src.readInt();
        this.nodes = new long[count];
        for (int i = 0; i < count; i++) {
            this.nodes[i] = src.readLong();
        }

        super.fromBytes(src);
    }

    @Override
    public Area toArea(@NonNull Storage storage) throws Exception {
        if (this.nodes.length < 3) { //less than 3 points -> it can't be a valid polygon
            return null;
        }

        int count = this.nodes.length;
        if (this.nodes[0] != this.nodes[count - 1]) { //first and last points aren't the same -> not a closed way
            return null;
        }

        if (!AreaKeys.isWayArea(this.tags)) { //this way's tags don't indicate that it's an area, don't bother making it into one
            return null;
        }

        //the resulting area consists of a single outer line, no holes, no multipolygons

        //box IDs
        List<Long> boxedIds = new ArrayList<>(count);
        for (int i = 0; i < count - 1; i++) { //skip last point because otherwise it'll be retrieved and deserialized twice
            boxedIds.add(this.nodes[i]);
        }

        //get nodes by their IDs
        List<Node> nodes = storage.nodes().getAll(boxedIds);

        //convert nodes to points
        double[][] outerRing = new double[count][];
        for (int i = 0; i < count - 1; i++) {
            outerRing[i] = nodes.get(i).toPoint();
        }
        outerRing[count - 1] = outerRing[0]; //set last point to first point

        return new Area(Area.elementIdToAreaId(this), new Shape[]{
                new Shape(outerRing, new double[0][][])
        });
    }
}
