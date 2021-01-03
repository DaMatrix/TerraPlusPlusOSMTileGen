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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.natives.PolygonAssembler;
import net.daporkchop.tpposmtilegen.osm.area.Area;
import net.daporkchop.tpposmtilegen.osm.area.AreaKeys;
import net.daporkchop.tpposmtilegen.osm.line.Line;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.Point;

import java.util.List;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString(callSuper = true)
public final class Way extends Element<Way> {
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
    public Way fromBytes(@NonNull ByteBuf src) {
        int count = src.readInt();
        this.nodes = new long[count];
        for (int i = 0; i < count; i++) {
            this.nodes[i] = src.readLong();
        }

        return super.fromBytes(src);
    }

    @Override
    public void computeReferences(@NonNull Storage storage) throws Exception {
        storage.references().addReferences(Node.TYPE, LongArrayList.wrap(this.nodes), Way.TYPE, this.id);
    }

    @Override
    public Geometry toGeometry(@NonNull Storage storage) throws Exception {
        int count = this.nodes.length;
        if (count <= 2) { //less than 4 points -> it can't be a valid geometry
            return null;
        }

        //get points by their IDs
        List<Point> points = storage.points().getAll(LongArrayList.wrap(this.nodes));

        for (int i = 0; i < count; i++) {
            if (points.get(i) == null) {
                System.err.printf("unknown node %d in area way %d\n", this.nodes[i], this.id);
                return null;
            }
        }

        Area area = this.toArea(count, points);
        return area == null
                ? new Line(addTypeToId(TYPE, this.id), this.tags, points.toArray(new Point[0])) //area assembly was unsuccessful
                : area;
    }

    private Area toArea(int count, List<Point> points) {
        if (this.nodes.length <= 3) { //less than 4 points -> it can't be a valid polygon
            return null;
        }

        if (this.nodes[0] != this.nodes[count - 1]) { //first and last points aren't the same -> not a closed way
            return null;
        }

        if (!AreaKeys.isWayArea(this.tags)) { //this way's tags don't indicate that it's an area, don't bother making it into one
            return null;
        }

        //copy points into direct memory so that they can be passed along to JNI
        long addr = PUnsafe.allocateMemory(count * PolygonAssembler.POINT_SIZE);
        try {
            for (int i = 0; i < count; i++) {
                PolygonAssembler.putPoint(addr + i * PolygonAssembler.POINT_SIZE, this.nodes[i], points.get(i));
            }

            return PolygonAssembler.assembleWay(addTypeToId(TYPE, this.id), this.tags, this.id, addr, count);
        } finally {
            PUnsafe.freeMemory(addr);
        }
    }
}
