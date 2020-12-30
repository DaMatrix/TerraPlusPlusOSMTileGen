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

package net.daporkchop.tpposmtilegen.storage;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.util.persistent.BufferedPersistentMap;
import net.daporkchop.tpposmtilegen.util.persistent.PersistentMap;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapBitSet;

import java.nio.file.Path;

/**
 * @author DaPorkchop_
 */
@Getter
public class Storage implements AutoCloseable {
    protected final PersistentMap<Long, Node> nodes;
    protected final PersistentMap<Long, Way> ways;
    protected final PersistentMap<Long, Relation> relations;

    protected final OffHeapBitSet nodeFlags;
    protected final OffHeapBitSet wayFlags;
    protected final OffHeapBitSet relationFlags;

    public Storage(@NonNull Path root) throws Exception {
        this.nodes = new BufferedPersistentMap<>(new NodeDB(root, "nodes"), 100_000);
        this.ways = new BufferedPersistentMap<>(new WayDB(root, "ways"), 10_000);
        this.relations = new BufferedPersistentMap<>(new RelationDB(root, "relations"), 10_000);

        this.nodeFlags = new OffHeapBitSet(root.resolve("nodeFlags"), 1L << 40L);
        this.wayFlags = new OffHeapBitSet(root.resolve("wayFlags"), 1L << 40L);
        this.relationFlags = new OffHeapBitSet(root.resolve("relationFlags"), 1L << 40L);
    }

    public void flush() throws Exception {
        this.nodes.flush();
        this.ways.flush();
        this.relations.flush();
    }

    @Override
    public void close() throws Exception {
        this.nodes.close();
        this.ways.close();
        this.relations.close();

        this.nodeFlags.close();
        this.wayFlags.close();
        this.relationFlags.close();
    }
}
