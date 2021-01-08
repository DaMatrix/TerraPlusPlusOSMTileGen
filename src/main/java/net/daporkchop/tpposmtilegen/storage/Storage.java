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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.osm.Coastline;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.map.BlobDB;
import net.daporkchop.tpposmtilegen.storage.map.CoastlineDB;
import net.daporkchop.tpposmtilegen.storage.map.LongArrayDB;
import net.daporkchop.tpposmtilegen.storage.map.NodeDB;
import net.daporkchop.tpposmtilegen.storage.map.PointDB;
import net.daporkchop.tpposmtilegen.storage.map.RelationDB;
import net.daporkchop.tpposmtilegen.storage.map.RocksDBMap;
import net.daporkchop.tpposmtilegen.storage.map.WayDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WriteBatch;
import net.daporkchop.tpposmtilegen.storage.special.ReferenceDB;
import net.daporkchop.tpposmtilegen.storage.special.TileDB;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Threading;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapAtomicBitSet;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapAtomicLong;
import org.rocksdb.CompressionType;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
public final class Storage implements AutoCloseable {
    protected NodeDB nodes;
    protected PointDB points;
    protected WayDB ways;
    protected RelationDB relations;
    protected CoastlineDB coastlines;
    protected final Int2ObjectMap<RocksDBMap<? extends Element>> elementsByType = new Int2ObjectOpenHashMap<>();

    protected final OffHeapAtomicBitSet nodeFlags;
    protected final OffHeapAtomicBitSet taggedNodeFlags;
    protected final OffHeapAtomicBitSet wayFlags;
    protected final OffHeapAtomicBitSet relationFlags;

    protected final OffHeapAtomicLong coastlineCount;

    protected final OffHeapAtomicLong sequenceNumber;
    protected final OffHeapAtomicLong replicationTimestamp;

    protected ReferenceDB references;
    protected TileDB tileContents;
    protected final OffHeapAtomicBitSet dirtyTiles;
    protected LongArrayDB intersectedTiles;
    protected BlobDB tempJsonStorage;

    protected final Database db;

    public Storage(@NonNull Path root) throws Exception {
        this.db = new Database.Builder()
                .add("nodes", (database, handle) -> this.nodes = new NodeDB(database, handle))
                .add("points", CompressionType.NO_COMPRESSION, (database, handle) -> this.points = new PointDB(database, handle))
                .add("ways", (database, handle) -> this.ways = new WayDB(database, handle))
                .add("relations", (database, handle) -> this.relations = new RelationDB(database, handle))
                .add("coastlines", (database, handle) -> this.coastlines = new CoastlineDB(database, handle))
                .add("references", (database, handle) -> this.references = new ReferenceDB(database, handle))
                .add("tiles", (database, handle) -> this.tileContents = new TileDB(database, handle))
                .add("intersected_tiles", (database, handle) -> this.intersectedTiles = new LongArrayDB(database, handle))
                .add("json", (database, handle) -> this.tempJsonStorage = new BlobDB(database, handle))
                .autoFlush(true)
                .build(root.resolve("db"));

        this.nodeFlags = new OffHeapAtomicBitSet(root.resolve("osm_nodeFlags"), 1L << 40L);
        this.taggedNodeFlags = new OffHeapAtomicBitSet(root.resolve("osm_taggedNodeFlags"), 1L << 40L);
        this.wayFlags = new OffHeapAtomicBitSet(root.resolve("osm_wayFlags"), 1L << 40L);
        this.relationFlags = new OffHeapAtomicBitSet(root.resolve("osm_relationFlags"), 1L << 40L);

        this.coastlineCount = new OffHeapAtomicLong(root.resolve("coastline_count"), 0L);

        this.sequenceNumber = new OffHeapAtomicLong(root.resolve("osm_sequenceNumber"), -1L);
        this.replicationTimestamp = new OffHeapAtomicLong(root.resolve("osm_replicationTimestamp"), -1L);

        this.dirtyTiles = new OffHeapAtomicBitSet(root.resolve("tiles_dirty"), 1L << 40L);

        this.elementsByType.put(Node.TYPE, this.nodes);
        this.elementsByType.put(Way.TYPE, this.ways);
        this.elementsByType.put(Relation.TYPE, this.relations);
        this.elementsByType.put(Coastline.TYPE, this.coastlines);
    }

    public void putNode(@NonNull WriteBatch batch, @NonNull Node node, @NonNull Point point) throws Exception {
        this.points.put(batch, node.id(), point);
        this.nodeFlags.set(node.id());

        if (!node.tags().isEmpty()) {
            this.nodes.put(batch, node.id(), node);
            this.taggedNodeFlags.set(node.id());
        }
    }

    public void putWay(@NonNull WriteBatch batch, @NonNull Way way) throws Exception {
        this.ways.put(batch, way.id(), way);
        this.wayFlags.set(way.id());
    }

    public void putRelation(@NonNull WriteBatch batch, @NonNull Relation relation) throws Exception {
        this.relations.put(batch, relation.id(), relation);
        this.relationFlags.set(relation.id());
    }

    public Spliterator.OfLong[] spliterateElements(boolean taggedNodes, boolean ways, boolean relations, boolean coastlines) {
        @RequiredArgsConstructor
        class TypeIdAddingSpliterator implements Spliterator.OfLong {
            protected final Spliterator.OfLong delegate;
            protected final int type;

            @Override
            public OfLong trySplit() {
                OfLong delegateSplit = this.delegate.trySplit();
                return delegateSplit != null ? new TypeIdAddingSpliterator(delegateSplit, this.type) : null;
            }

            @Override
            public boolean tryAdvance(@NonNull LongConsumer action) {
                return this.delegate.tryAdvance((LongConsumer) id -> action.accept(Element.addTypeToId(this.type, id)));
            }

            @Override
            public void forEachRemaining(@NonNull LongConsumer action) {
                this.delegate.forEachRemaining((LongConsumer) id -> action.accept(Element.addTypeToId(this.type, id)));
            }

            @Override
            public long estimateSize() {
                return this.delegate.estimateSize();
            }

            @Override
            public int characteristics() {
                return this.delegate.characteristics();
            }

            @Override
            public long getExactSizeIfKnown() {
                return this.delegate.getExactSizeIfKnown();
            }
        }

        List<Spliterator.OfLong> list = new ArrayList<>(3);
        if (taggedNodes) {
            list.add(new TypeIdAddingSpliterator(this.taggedNodeFlags.spliterator(), Node.TYPE));
        }
        if (ways) {
            list.add(new TypeIdAddingSpliterator(this.wayFlags.spliterator(), Way.TYPE));
        }
        if (relations) {
            list.add(new TypeIdAddingSpliterator(this.relationFlags.spliterator(), Relation.TYPE));
        }
        if (coastlines) {
            list.add(new TypeIdAddingSpliterator(LongStream.range(0L, this.coastlineCount.get()).spliterator(), Coastline.TYPE));
        }
        return list.toArray(new Spliterator.OfLong[0]);
    }

    public Element getElement(long combinedId) throws Exception {
        int type = Element.extractType(combinedId);
        long id = Element.extractId(combinedId);
        RocksDBMap<? extends Element> map = this.elementsByType.getOrDefault(type, null);
        checkArg(map != null, "unknown element type %d (id %d)", type, id);
        return map.get(id);
    }

    public void convertToGeoJSONAndStoreInDB(@NonNull WriteBatch batch, @NonNull Path outputRoot, long combinedId) throws Exception {
        int type = Element.extractType(combinedId);
        String typeName = Element.typeName(type);
        long id = Element.extractId(combinedId);

        Element element = this.getElement(combinedId);
        checkState(element != null, "unknown %s %d", typeName, id);

        Geometry geometry = element.toGeometry(this);
        if (geometry != null) {
            long[] arr = geometry.listIntersectedTiles();
            int tileCount = arr.length;

            if (tileCount == 0) { //nothing to do
                return;
            } else if (tileCount == 1) { //mark all tiles as dirty
                this.dirtyTiles.set(arr[0]);
            } else { //tileCount is > 1: use a fast method that flips multiple bits at once
                Arrays.sort(arr);
                long rangeBegin = -1L;
                long next = -1L;
                for (int i = 0; i < tileCount; i++) {
                    long v = arr[i];
                    if (rangeBegin < 0L) {
                        rangeBegin = v;
                        next = v + 1L;
                    } else if (next == v) {
                        next = v + 1L;
                    } else {
                        this.dirtyTiles.set(rangeBegin, next);
                        rangeBegin = next = -1L;
                    }
                }
                if (rangeBegin >= 0L) {
                    this.dirtyTiles.set(rangeBegin, next);
                }
            }

            this.tileContents.addElementToTiles(batch, LongArrayList.wrap(arr), type, id); //add this element to all tiles
            this.intersectedTiles.put(batch, combinedId, arr);

            //encode geometry to GeoJSON
            StringBuilder builder = new StringBuilder();

            Geometry.toGeoJSON(builder, geometry, element.tags(), combinedId);

            //convert json to bytes
            ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.ioBuffer(builder.length());
            try {
                buf.writeCharSequence(builder, StandardCharsets.US_ASCII);
                if (!geometry.shouldStoreExternally(tileCount, buf.readableBytes())) { //the element's geometry is small enough that storing it in multiple tiles should be a non-issue
                    this.tempJsonStorage.put(batch, combinedId, buf.internalNioBuffer(0, buf.readableBytes()));
                } else { //element is referenced multiple times, store it in an external file
                    String path = geometry.externalStoragePath(type, id);
                    Path file = outputRoot.resolve(path);

                    //ensure directory exists
                    Files.createDirectories(file.getParent());

                    //write to file
                    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                        buf.readBytes(channel, buf.readableBytes());
                    }

                    //create reference object and store it in db
                    builder = new StringBuilder();
                    builder.append("{\"type\":\"Reference\",\"location\":\"").append(path).append("\"}\n");
                    buf.clear().writeCharSequence(builder, StandardCharsets.US_ASCII);
                    this.tempJsonStorage.put(batch, combinedId, buf.internalNioBuffer(0, buf.readableBytes()));
                }
            } finally {
                buf.release();
            }
        }
    }

    public void exportDirtyTiles(@NonNull Path outputRoot) throws Exception {
        try (ProgressNotifier notifier = new ProgressNotifier("Write tiles: ", 5000L, "tiles")) {
            Threading.forEachParallelLong(tilePos -> {
                int tileX = Tile.tileX(tilePos);
                int tileY = Tile.tileY(tilePos);
                try {
                    LongList elements = new LongArrayList();
                    this.tileContents.getElementsInTile(tilePos, elements);
                    if (elements.isEmpty()) { //nothing to write
                        return;
                    }

                    Path dir = outputRoot.resolve("tile").resolve(String.valueOf(tileX));
                    Files.createDirectories(dir);
                    try (FileChannel channel = FileChannel.open(dir.resolve(tileY + ".json"), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                        channel.write(this.tempJsonStorage.getAll(elements).toArray(new ByteBuffer[0]));
                    }
                } catch (Exception e) {
                    throw new RuntimeException("tile " + tileX + ' ' + tileY, e);
                }
                notifier.step(0);
            }, this.dirtyTiles.spliterator());
            this.dirtyTiles.clear();
            this.flush();
        }
    }

    public void purge(boolean temp, boolean index) throws Exception {
        if (!temp && !index) { //do nothing?!?
            return;
        }

        System.out.println("Cleaning up... (this might take a while)");
        this.flush();
        if (temp) {
            System.out.println("Clearing temporary GeoJSON storage...");
            this.tempJsonStorage.clear(this.db.batch());
        }
        if (index) {
            System.out.println("Clearing tile index...");
            this.tileContents.clear(this.db.batch());
            System.out.println("Clearing dirty tile flags...");
            this.dirtyTiles.clear();
        }
        System.out.println("Cleared.");
    }

    public void flush() throws Exception {
        this.db.flush();
    }

    @Override
    public void close() throws Exception {
        this.nodeFlags.close();
        this.taggedNodeFlags.close();
        this.wayFlags.close();
        this.relationFlags.close();

        this.coastlineCount.close();

        this.sequenceNumber.close();
        this.replicationTimestamp.close();

        this.dirtyTiles.close();

        this.db.close();
    }
}
