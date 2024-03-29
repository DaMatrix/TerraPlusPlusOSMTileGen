/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
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
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.lib.common.annotation.param.NotNegative;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Area;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Line;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.natives.PolygonAssembler;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;

import java.util.List;
import java.util.Map;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
@Getter
@ToString(callSuper = true)
public final class Way extends Element {
    public static final int TYPE = Element.WAY_TYPE;

    protected long[] nodes;

    public Way(long id, Map<String, String> tags, int version, boolean visible, @NonNull long[] nodes) {
        super(id, tags, version, visible);

        this.nodes = nodes;
    }

    public Way(@NonNull com.wolt.osm.parallelpbf.entity.Way way) {
        super(way);

        List<Long> nodesList = way.getNodes();
        this.nodes = new long[nodesList.size()];
        for (int i = 0; i < this.nodes.length; i++) {
            this.nodes[i] = nodesList.get(i);
        }
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
        super.toBytes(dst);

        dst.writeInt(this.nodes.length);
        for (long node : this.nodes) {
            dst.writeLong(node);
        }
    }

    @Override
    public void fromBytes(@NonNull ByteBuf src) {
        super.fromBytes(src);

        int count = src.readInt();
        this.nodes = new long[count];
        for (int i = 0; i < count; i++) {
            this.nodes[i] = src.readLong();
        }
    }

    @Override
    public LongList getReferencesCombinedIds() {
        checkState(Element.addTypeToId(Node.TYPE, 0L) == 0L);
        return LongLists.unmodifiable(LongArrayList.wrap(this.nodes));
    }

    @Override
    public boolean allowedToIncludeAtLevel(@NotNegative int level) {
        //TODO: smarter logic for determining this
        return super.allowedToIncludeAtLevel(level) && true;
    }

    @Override
    public Geometry toGeometry(@NonNull Storage storage, @NonNull DBReadAccess access) throws Exception {
        int count = this.nodes.length;
        if (count < 2) { //less than 2 points -> it can't be a valid geometry
            return null;
        }

        if ("coastline".equals(this.tags.get("natural"))) { //skip coastlines because they're processed separately
            return null;
        }

        //get points by their IDs
        List<Point> points = storage.points().getAll(access, LongArrayList.wrap(this.nodes));

        for (int i = 0; i < count; i++) {
            if (points.get(i) == null) {
                logger.warn("unknown node %d in area way %d", this.nodes[i], this.id);
                return null;
            }
        }

        Area area = this.toArea(count, points);
        return area == null
                ? new Line(points.toArray(new Point[0])) //area assembly was unsuccessful
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

            return PolygonAssembler.assembleWay(this.id, addr, count);
        } finally {
            PUnsafe.freeMemory(addr);
        }
    }

    @Override
    public void erase() {
        super.erase();
        this.nodes = null;
    }
}
