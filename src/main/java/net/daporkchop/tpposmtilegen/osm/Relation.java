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

import java.nio.charset.StandardCharsets;
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
