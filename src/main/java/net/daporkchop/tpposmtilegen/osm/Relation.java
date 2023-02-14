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

import com.wolt.osm.parallelpbf.entity.RelationMember;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.natives.PolygonAssembler;
import net.daporkchop.tpposmtilegen.osm.changeset.Changeset;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString(callSuper = true)
public final class Relation extends Element {
    public static final int TYPE = 2;

    @NonNull
    protected Member[] members;

    public Relation(long id, Map<String, String> tags, @NonNull Member[] members) {
        super(id, tags);

        this.members = members;
    }

    public Relation(long id, ByteBuf data) {
        super(id, data);
    }

    public Relation tags(@NonNull Map<String, String> tags) {
        super.tags = tags;
        return this;
    }

    @Override
    public int type() {
        return TYPE;
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeInt(this.members.length);
        for (Member member : this.members) {
            member.write(dst);
        }

        super.toBytes(dst);
    }

    @Override
    public void fromBytes(@NonNull ByteBuf src) {
        int count = src.readInt();
        this.members = new Member[count];
        for (int i = 0; i < count; i++) {
            this.members[i] = new Member(src);
        }

        super.fromBytes(src);
    }

    @Override
    public void computeReferences(@NonNull DBAccess access, @NonNull Storage storage) throws Exception {
        LongList ids = new LongArrayList(this.members.length);
        for (Member member : this.members) {
            ids.add(member.combinedId);
        }

        //first parameter (type) is 0 because the ids are already combined with their type
        storage.references().addReferences(access, ids, addTypeToId(TYPE, this.id));
    }

    @Override
    public Geometry toGeometry(@NonNull Storage storage, @NonNull DBAccess access) throws Exception {
        //we don't care about any relation that isn't an area

        if (!AreaKeys.isRelationArea(this.tags)) { //this relations's tags don't indicate that it's an area, don't bother making it into one
            return null;
        }

        List<Member> wayMembers = new ArrayList<>(this.members.length);
        for (Member member : this.members) {
            if (member.getType() == Way.TYPE) {
                wayMembers.add(member);
            } else {
                logger.warn("skipping invalid member of type %d (%s) in area relation %d",
                        member.getType(), typeName(member.getType()), this.id);
            }
        }
        int wayCount = wayMembers.size();

        //scan relation members
        long[] wayIds = new long[wayCount];
        byte[] roles = new byte[wayCount];
        for (int i = 0; i < wayCount; i++) {
            Member member = wayMembers.get(i);
            wayIds[i] = member.getId();
            switch (member.role) {
                case "outer":
                    roles[i] = 0;
                    break;
                case "inner":
                    roles[i] = 1;
                    break;
                case "":
                    roles[i] = 2;
                    break;
                default:
                    roles[i] = 3;
            }
        }

        //load ways from db
        List<Way> ways = storage.ways().getAll(access, LongArrayList.wrap(wayIds));

        //ensure all ways exist
        for (int i = 0; i < wayCount; i++) {
            if (ways.get(i) == null) {
                logger.warn("unknown way %d in area relation %d", wayIds[i], this.id);
                return null;
            }
        }

        //gather all point IDs into single array
        int[] coordCounts = ways.stream().mapToInt(way -> way.nodes().length).toArray();
        int pointCount = Arrays.stream(coordCounts).sum();
        long[] pointIds = new long[pointCount];
        for (int i = 0, off = 0; i < wayCount; i++) {
            long[] nodes = ways.get(i).nodes();
            System.arraycopy(nodes, 0, pointIds, off, nodes.length);
            off += nodes.length;
        }

        //load points from db
        List<Point> points = storage.points().getAll(access, LongArrayList.wrap(pointIds));

        //copy points into direct memory so that they can be passed along to JNI
        long[] coordAddrs = Arrays.stream(coordCounts).mapToLong(i -> i * PolygonAssembler.POINT_SIZE).map(PUnsafe::allocateMemory).toArray();
        try {
            for (int w = 0, p = 0; w < wayCount; w++) {
                for (int n = 0, coordCount = coordCounts[w]; n < coordCount; n++, p++) {
                    if (points.get(p) == null) {
                        logger.warn("unknown node %d in way %d in area relation %d", pointIds[p], wayIds[w], this.id);
                        return null;
                    }
                    PolygonAssembler.putPoint(coordAddrs[w] + n * PolygonAssembler.POINT_SIZE, pointIds[p], points.get(p));
                }
            }

            return PolygonAssembler.assembleRelation(this.id, wayIds, coordAddrs, coordCounts, roles);
        } finally {
            Arrays.stream(coordAddrs).forEach(PUnsafe::freeMemory);
        }
    }

    /**
     * A relation member.
     *
     * @author DaPorkchop_
     */
    @AllArgsConstructor
    @Getter
    @ToString
    public static final class Member {
        static {
            checkState(Node.TYPE == RelationMember.Type.NODE.ordinal(), "node");
            checkState(Way.TYPE == RelationMember.Type.WAY.ordinal(), "way");
            checkState(Relation.TYPE == RelationMember.Type.RELATION.ordinal(), "relation");
        }

        private static String readNullableString(ByteBuf src) {
            int size = src.readInt();
            return size >= 0 ? src.readCharSequence(size, StandardCharsets.UTF_8).toString() : null;
        }

        protected final long combinedId;
        protected final String role;

        protected Member(ByteBuf src) {
            this(src.readLong(), readNullableString(src));
        }

        public Member(@NonNull RelationMember osm) {
            long id = osm.getId();
            this.combinedId = addTypeToId(osm.getType().ordinal(), id);
            this.role = osm.getRole();
        }

        public Member(@NonNull Changeset.Relation.Member change) {
            long id = change.ref();
            int type;
            switch (change.type()) {
                case "node":
                    type = Node.TYPE;
                    break;
                case "way":
                    type = Way.TYPE;
                    break;
                case "relation":
                    type = Relation.TYPE;
                    break;
                default:
                    throw new IllegalArgumentException(change.type());
            }
            this.combinedId = addTypeToId(type, id);
            this.role = change.role();
        }

        public long getId() {
            return extractId(this.combinedId);
        }

        public int getType() {
            return extractType(this.combinedId);
        }

        protected void write(ByteBuf dst) {
            dst.writeLong(this.combinedId);
            int sizeIndex = dst.writerIndex();
            dst.writeInt(-1);
            if (this.role != null) {
                dst.setInt(sizeIndex, dst.writeCharSequence(this.role, StandardCharsets.UTF_8));
            }
        }
    }
}
