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

package net.daporkchop.tpposmtilegen.osm;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.util.Persistent;

import java.util.Map;

/**
 * An OpenStreetMap element.
 *
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
@Getter
@ToString
public abstract class Element implements Persistent {
    public static String typeName(int type) {
        switch (type) {
            case Node.TYPE:
                return "node";
            case Way.TYPE:
                return "way";
            case Relation.TYPE:
                return "relation";
            case Coastline.TYPE:
                return "coastline";
            default:
                return "unknown";
        }
    }

    public static long addTypeToId(int type, long id) {
        return (id << 2L) | type;
    }

    public static long extractId(long combined) {
        return combined >>> 2L;
    }

    public static int extractType(long combined) {
        return (int) (combined & 3);
    }

    protected final long id;

    @NonNull
    protected Map<String, String> tags;

    public Element(long id, @NonNull ByteBuf data) {
        this.id = id;
        this.fromBytes(data);
    }

    public abstract int type();

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        Persistent.writeTags(dst, this.tags);
    }

    protected void fromBytes(@NonNull ByteBuf src) {
        this.tags = Persistent.readTags(src);
    }

    public abstract void computeReferences(@NonNull DBAccess access, @NonNull Storage storage) throws Exception;

    public abstract Geometry toGeometry(@NonNull Storage storage, @NonNull DBAccess access) throws Exception;
}
