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

import com.wolt.osm.parallelpbf.entity.RelationMember;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.primitive.map.IntObjMap;
import net.daporkchop.lib.primitive.map.LongObjMap;
import net.daporkchop.lib.primitive.map.open.IntObjOpenHashMap;
import net.daporkchop.lib.primitive.map.open.LongObjOpenHashMap;
import net.daporkchop.tpposmtilegen.osm.area.Area;
import net.daporkchop.tpposmtilegen.osm.area.AreaKeys;
import net.daporkchop.tpposmtilegen.osm.area.Shape;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
@Setter
@ToString(callSuper = true)
public final class Relation extends Element {
    public static final int TYPE = 2;

    private static final long TYPE_SHIFT = 62L;
    private static final long TYPE_MASK = 3L;

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
    public Area toArea(@NonNull Storage storage) throws Exception {
        if (!AreaKeys.isRelationArea(this.tags)) { //this relations's tags don't indicate that it's an area, don't bother making it into one
            return null;
        }

        {
            boolean hasOuter = false;
            for (Member member : this.members) {
                int type = member.getType();
                checkState(type == Way.TYPE, "invalid member of type %d in area relation %d", type, this.id);
                if ("outer".equals(member.role)) {
                    hasOuter = true;
                } else if ("inner".equals(member.role)) {
                    checkState(hasOuter, "relation %d has inner loop before outer!", this.id);
                } else {
                    return null;
                }
            }
            if (!hasOuter) { //relation has no outer loops, skip
                return null;
            }
        }

        List<Shape> shapes = new ArrayList<>();
        double[][] outerRing = null;
        List<double[][]> linesOut = new ArrayList<>();
        List<Long> boxedIds = new ArrayList<>();
        LongObjMap<long[]> endsToWays = new LongObjOpenHashMap<>();

        boolean prevType = false;
        for (Member member : this.members) {
            boolean currType = "outer".equals(member.role);
            if (currType != prevType && !endsToWays.isEmpty()) {
                throw new IllegalStateException(PStrings.fastFormat("transitioned from outer:%v to outer:%b, but there were still unused ways!", prevType, currType));
            }

            long[] ids = storage.ways().get(member.getId()).nodes;
            if (ids[0] != ids[ids.length - 1]) {
                long[] neighborFront = endsToWays.get(ids[0]);
                if (neighborFront != null) { //merge with other way at front
                    long[] newArr = new long[(neighborFront.length - 1) + ids.length];
                    System.arraycopy(neighborFront, 0, newArr, 0, neighborFront.length - 1);
                    System.arraycopy(ids, 0, newArr, neighborFront.length - 1, ids.length);
                    ids = newArr;
                    checkState(endsToWays.replace(neighborFront[0], neighborFront, ids));
                    checkState(endsToWays.remove(ids[0], neighborFront));
                    endsToWays.put(ids[ids.length - 1], ids);
                }

                if (ids[0] != ids[ids.length - 1]) {
                    long[] neighborBack = endsToWays.get(ids[ids.length - 1]);
                    if (neighborBack != null) { //merge with other way at back
                        long[] newArr = new long[(ids.length - 1) + neighborBack.length];
                        System.arraycopy(ids, 0, newArr, 0, ids.length - 1);
                        System.arraycopy(neighborBack, 0, newArr, ids.length - 1, neighborBack.length);
                        ids = newArr;
                        checkState(endsToWays.replace(neighborBack[neighborBack.length - 1], neighborBack, ids));
                        checkState(endsToWays.remove(ids[ids.length - 1], neighborBack));
                        endsToWays.put(ids[0], ids);
                    }
                }
            }

            if (ids[0] == ids[ids.length - 1]) { //the line is closed, emit it
                //remove end mappings
                endsToWays.remove(ids[0], ids);
                endsToWays.remove(ids[ids.length - 1], ids);

                //box IDs
                for (int i = 0; i < ids.length - 1; i++) { //skip last point because otherwise it'll be retrieved and deserialized twice
                    boxedIds.add(ids[i]);
                }

                //get nodes by their IDs
                List<Node> nodes = storage.nodes().getAll(boxedIds);

                //convert nodes to points
                double[][] ring = new double[ids.length][];
                for (int i = 0; i < ids.length - 1; i++) {
                    ring[i] = nodes.get(i).toPoint();
                }
                ring[ids.length - 1] = ring[0]; //set last point to first point

                if (currType) {
                    if (outerRing != null) { //beginning a second outer ring, flush currently pending ring
                        shapes.add(new Shape(outerRing, linesOut.toArray(new double[0][][])));
                        linesOut.clear();
                    }
                    outerRing = ring;
                } else {
                    linesOut.add(ring);
                }
            }

            prevType = currType;
        }

        if (!endsToWays.isEmpty()) {
            throw new IllegalStateException(PStrings.fastFormat("transitioned from outer:%v to end, but there were still unused ways!", prevType));
        }
        checkState(outerRing != null, "no geometry left at end?!?");

        shapes.add(new Shape(outerRing, linesOut.toArray(new double[0][][])));

        return new Area(Area.elementIdToAreaId(this), shapes.toArray(new Shape[0]));
    }

    /**
     * A relation member.
     *
     * @author DaPorkchop_
     */
    @AllArgsConstructor
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

        protected final long id;
        @Getter
        protected final String role;

        protected Member(ByteBuf src) {
            this(src.readLong(), readNullableString(src));
        }

        public Member(@NonNull RelationMember osm) {
            long id = osm.getId();
            checkArg((id >>> TYPE_SHIFT) == 0L, "element has id with top 2 bits used: %s", osm);
            this.id = id | ((long) osm.getType().ordinal() << TYPE_SHIFT);
            this.role = osm.getRole();
        }

        public long getId() {
            return this.id & ~(TYPE_MASK << TYPE_SHIFT);
        }

        public int getType() {
            return (int) (this.id >>> TYPE_SHIFT);
        }

        protected void write(ByteBuf dst) {
            dst.writeLong(this.id);
            int sizeIndex = dst.writerIndex();
            dst.writeInt(-1);
            if (this.role != null) {
                dst.setInt(sizeIndex, dst.writeCharSequence(this.role, StandardCharsets.UTF_8));
            }
        }
    }
}
