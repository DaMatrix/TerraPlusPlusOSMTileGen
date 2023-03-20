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

import com.wolt.osm.parallelpbf.entity.OsmEntity;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.lib.common.annotation.param.NotNegative;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.Persistent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * An OpenStreetMap element.
 *
 * @author DaPorkchop_
 */
@Getter
@ToString
public abstract class Element implements Persistent {
    private static final List<String> TYPE_NAMES = Collections.unmodifiableList(Arrays.asList("node", "way", "relation", "coastline"));

    protected static final int NODE_TYPE = 0;
    protected static final int WAY_TYPE = 1;
    protected static final int RELATION_TYPE = 2;
    protected static final int COASTLINE_TYPE = 3;

    public static List<String> typeNames() {
        return TYPE_NAMES;
    }

    public static String typeName(int type) {
        return TYPE_NAMES.get(type);
    }

    public static int typeId(@NonNull String typeName) {
        int typeId = TYPE_NAMES.indexOf(typeName);
        checkArg(typeId >= 0, typeName);
        return typeId;
    }

    public static int typeCount() {
        return TYPE_NAMES.size();
    }

    public static long addTypeToId(int type, long id) {
        return ((long) type << 62L) | id;
    }

    public static long extractId(long combined) {
        return combined & ~(3L << 62L);
    }

    public static int extractType(long combined) {
        return (int) (combined >>> 62L);
    }

    protected final long id;
    protected Map<String, String> tags;
    protected int version;

    /**
     * The OpenStreetMap 'visible' property.
     * <p>
     * This is {@code false} if the element is currently deleted, but even a value of {@code true} does not necessarily mean that the element is actually visible.
     */
    protected boolean visible;

    public Element(long id, @NonNull Map<String, String> tags, int version, boolean visible) {
        this.id = id;
        this.tags = tags;
        this.version = version;
        this.visible = visible;
    }

    public Element(@NonNull OsmEntity entity) {
        this(entity.getId(), entity.getTags().isEmpty() ? Collections.emptyMap() : entity.getTags(), entity.getInfo().getVersion(), entity.getInfo().isVisible());
    }

    public Element(long id, @NonNull ByteBuf data) {
        this.id = id;
        this.fromBytes(data);
    }

    public abstract int type();

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeInt(this.version);
        dst.writeBoolean(this.visible);
        Persistent.writeTags(dst, this.tags);
    }

    protected void fromBytes(@NonNull ByteBuf src) {
        this.version = src.readInt();
        this.visible = src.readBoolean();
        this.tags = Persistent.readTags(src);
    }

    public void computeReferences(@NonNull DBWriteAccess access, @NonNull Storage storage) throws Exception {
        storage.references().addReferences(access, this.getReferencesCombinedIds(), addTypeToId(this.type(), this.id));
    }

    /**
     * @return a {@link LongList} containing the combined IDs of all elements referenced by this element
     */
    public abstract LongList getReferencesCombinedIds();

    /**
     * Checks if this element is allowed to be included in the tile data at the given detail level.
     *
     * @param level the detail level
     * @return {@code true} if this element is allowed to be included in the tile data at the given detail level
     */
    public boolean allowedToIncludeAtLevel(@NotNegative int level) {
        notNegative(level, "level");
        return this.visible();
    }

    /**
     * Assembles this element into its {@link Geometry} representation.
     *
     * @param storage the {@link Storage} which this element is stored in
     * @param access  the {@link DBReadAccess} to use for reading additional data required to assemble this element into its geometry representation
     * @return the assembled geometry, or {@code null} if this element couldn't be assembled
     */
    public abstract Geometry toGeometry(@NonNull Storage storage, @NonNull DBReadAccess access) throws Exception;

    /**
     * Clears object references held by this element instance in an effort to help the garbage collector.
     * <p>
     * Once this method has been called, accessing this instance in any way will result in undefined behavior!
     */
    public void erase() {
        this.tags.clear();
        this.tags = null;
    }
}
