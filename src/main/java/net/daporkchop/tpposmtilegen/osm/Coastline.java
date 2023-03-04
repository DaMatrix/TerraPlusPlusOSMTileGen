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
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Shape;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;

import java.util.Collections;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
public final class Coastline extends Element {
    private static final Map<String, String> TAGS = Collections.singletonMap("natural", "coastline");
    public static final int TYPE = 3;

    protected Area area;

    public Coastline(long id, @NonNull Area area) {
        super(id, TAGS, 0, true);

        this.area = area;
    }

    public Coastline(long id, @NonNull ByteBuf data) {
        super(id, data);

        this.tags = TAGS;
    }

    @Override
    public int type() {
        return TYPE;
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        this.area.toBytes(dst);
    }

    @Override
    protected void fromBytes(@NonNull ByteBuf src) {
        this.area = new Area(src);
    }

    @Override
    public LongList getReferencesCombinedIds() {
        //coastline isn't a real OpenStreetMap primitive type, so it has no references
        return LongLists.EMPTY_LIST;
    }

    @Override
    public Geometry toGeometry(@NonNull Storage storage, @NonNull DBReadAccess access) throws Exception {
        return this.area;
    }

    /**
     * Wrapper around the standard {@link net.daporkchop.tpposmtilegen.geometry.Area} class in order to define custom behaviors for coastlines.
     *
     * @author DaPorkchop_
     */
    public static final class Area extends net.daporkchop.tpposmtilegen.geometry.Area {
        public Area(@NonNull Shape[] shapes) {
            super(shapes);
        }

        public Area(@NonNull ByteBuf src) {
            super(src);
        }

        @Override
        public boolean shouldStoreExternally(int tiles, int dataSize) {
            return true;
        }
    }
}
