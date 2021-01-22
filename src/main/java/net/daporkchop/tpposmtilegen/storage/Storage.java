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

package net.daporkchop.tpposmtilegen.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.oio.StreamUtil;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.osm.Coastline;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.osm.changeset.Changeset;
import net.daporkchop.tpposmtilegen.osm.changeset.ChangesetState;
import net.daporkchop.tpposmtilegen.storage.map.BlobDB;
import net.daporkchop.tpposmtilegen.storage.map.CoastlineDB;
import net.daporkchop.tpposmtilegen.storage.map.LongArrayDB;
import net.daporkchop.tpposmtilegen.storage.map.NodeDB;
import net.daporkchop.tpposmtilegen.storage.map.PointDB;
import net.daporkchop.tpposmtilegen.storage.map.RelationDB;
import net.daporkchop.tpposmtilegen.storage.map.RocksDBMap;
import net.daporkchop.tpposmtilegen.storage.map.WayDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.special.DBLong;
import net.daporkchop.tpposmtilegen.storage.special.DirtyTracker;
import net.daporkchop.tpposmtilegen.storage.special.ReferenceDB;
import net.daporkchop.tpposmtilegen.storage.special.TileDB;
import net.daporkchop.tpposmtilegen.util.CloseableExecutor;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapAtomicLong;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapSpliteratableLongList;
import org.rocksdb.CompressionType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

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

    protected final OffHeapSpliteratableLongList unprocessedElements;

    protected DBLong sequenceNumber;
    protected final OffHeapAtomicLong replicationTimestamp;
    protected String replicationBaseUrl;

    protected ReferenceDB references;
    protected TileDB tileContents;
    protected DirtyTracker dirtyTiles;
    protected LongArrayDB intersectedTiles;
    protected BlobDB tempJsonStorage;

    protected final Database db;

    protected final Path root;

    public Storage(@NonNull Path root) throws Exception {
        this.root = root;

        this.db = new Database.Builder()
                .add("nodes", (database, handle) -> this.nodes = new NodeDB(database, handle))
                .add("points", (database, handle) -> this.points = new PointDB(database, handle))
                .add("ways", (database, handle) -> this.ways = new WayDB(database, handle))
                .add("relations", (database, handle) -> this.relations = new RelationDB(database, handle))
                .add("coastlines", (database, handle) -> this.coastlines = new CoastlineDB(database, handle))
                .add("references", (database, handle) -> this.references = new ReferenceDB(database, handle))
                .add("tiles", CompressionType.NO_COMPRESSION, (database, handle) -> this.tileContents = new TileDB(database, handle))
                .add("intersected_tiles", (database, handle) -> this.intersectedTiles = new LongArrayDB(database, handle))
                .add("dirty_tiles", CompressionType.NO_COMPRESSION, (database, handle) -> this.dirtyTiles = new DirtyTracker(database, handle))
                .add("json", CompressionType.NO_COMPRESSION, (database, handle) -> this.tempJsonStorage = new BlobDB(database, handle))
                .add("sequence_number", (database, handle) -> this.sequenceNumber = new DBLong(database, handle))
                .autoFlush(true)
                .build(root.resolve("db"));

        this.replicationTimestamp = new OffHeapAtomicLong(root.resolve("osm_replicationTimestamp"), -1L);
        this.unprocessedElements = new OffHeapSpliteratableLongList(root.resolve("unprocessedElements"));

        this.elementsByType.put(Node.TYPE, this.nodes);
        this.elementsByType.put(Way.TYPE, this.ways);
        this.elementsByType.put(Relation.TYPE, this.relations);
        this.elementsByType.put(Coastline.TYPE, this.coastlines);

        Path replicationUrlFile = root.resolve("replication_base_url.txt");
        if (Files.exists(replicationUrlFile)) {
            this.replicationBaseUrl = new String(Files.readAllBytes(replicationUrlFile)).trim();
        } else {
            this.setReplicationBaseUrl("https://planet.openstreetmap.org/replication/minute/");
        }
    }

    public void putNode(@NonNull DBAccess access, @NonNull Node node, @NonNull Point point) throws Exception {
        this.points.put(access, node.id(), point);

        if (!node.tags().isEmpty()) {
            this.nodes.put(access, node.id(), node);
            this.unprocessedElements.add(Element.addTypeToId(Node.TYPE, node.id()));
        }
    }

    public void putWay(@NonNull DBAccess access, @NonNull Way way) throws Exception {
        this.ways.put(access, way.id(), way);
        this.unprocessedElements.add(Element.addTypeToId(Way.TYPE, way.id()));
    }

    public void putRelation(@NonNull DBAccess access, @NonNull Relation relation) throws Exception {
        this.relations.put(access, relation.id(), relation);
        this.unprocessedElements.add(Element.addTypeToId(Relation.TYPE, relation.id()));
    }

    public Element getElement(@NonNull DBAccess access, long combinedId) throws Exception {
        int type = Element.extractType(combinedId);
        long id = Element.extractId(combinedId);
        RocksDBMap<? extends Element> map = this.elementsByType.getOrDefault(type, null);
        checkArg(map != null, "unknown element type %d (id %d)", type, id);
        return map.get(access, id);
    }

    public void convertToGeoJSONAndStoreInDB(@NonNull DBAccess access, @NonNull Path outputRoot, long combinedId) throws Exception {
        int type = Element.extractType(combinedId);
        String typeName = Element.typeName(type);
        long id = Element.extractId(combinedId);

        Element element = this.getElement(this.db.read(), combinedId);
        checkState(element != null, "unknown %s %d", typeName, id);

        Geometry geometry = element.toGeometry(this, this.db.read());
        if (geometry != null) {
            long[] arr = geometry.listIntersectedTiles();
            int tileCount = arr.length;

            if (tileCount == 0) { //nothing to do
                return;
            }

            this.dirtyTiles.markDirty(access, LongArrayList.wrap(arr));
            this.tileContents.addElementToTiles(access, LongArrayList.wrap(arr), combinedId); //add this element to all tiles
            this.intersectedTiles.put(access, combinedId, arr);

            //encode geometry to GeoJSON
            StringBuilder builder = new StringBuilder();

            Geometry.toGeoJSON(builder, geometry, element.tags(), combinedId);

            //convert json to bytes
            ByteBuffer buffer = Geometry.toBytes(builder);
            try {
                if (!geometry.shouldStoreExternally(tileCount, buffer.remaining())) {
                    //the element's geometry is small enough that storing it in multiple tiles should be a non-issue
                    this.tempJsonStorage.put(access, combinedId, buffer);
                } else { //element is referenced multiple times, store it in an external file
                    ByteBuffer reference = this.writeExternal(outputRoot, geometry, combinedId, buffer);
                    try {
                        //store reference object in geometry database
                        this.tempJsonStorage.put(access, combinedId, reference);
                    } finally {
                        PUnsafe.pork_releaseBuffer(reference);
                    }
                }
            } finally {
                PUnsafe.pork_releaseBuffer(buffer);
            }
        }
    }

    public ByteBuffer writeExternal(@NonNull Path outputRoot, @NonNull Geometry geometry, long combinedId, @NonNull ByteBuffer fullJson) throws IOException {
        String location = geometry.externalStorageLocation(Element.extractType(combinedId), Element.extractId(combinedId));
        Path file = outputRoot.resolve(location);

        //ensure directory exists
        Files.createDirectories(file.getParent());

        //write to file
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (fullJson.hasRemaining()) {
                channel.write(fullJson);
            }
        }

        //create reference object
        return Geometry.toBytes("{\"type\":\"Reference\",\"location\":\"" + location + "\"}\n");
    }

    public ByteBuffer createReference(@NonNull Geometry geometry, long combinedId) {
        String location = geometry.externalStorageLocation(Element.extractType(combinedId), Element.extractId(combinedId));

        return Geometry.toBytes("{\"type\":\"Reference\",\"location\":\"" + location + "\"}\n");
    }

    public Path externalFile(@NonNull Path outputRoot, @NonNull Geometry geometry, long combinedId) {
        return outputRoot.resolve(geometry.externalStorageLocation(Element.extractType(combinedId), Element.extractId(combinedId)));
    }

    public void exportDirtyTiles(@NonNull DBAccess access, @NonNull Path outputRoot) throws Exception {
        try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Write tiles")
                .slot("tiles", this.dirtyTiles.count(access))
                .build();
             CloseableExecutor executor = new CloseableExecutor("Tile write worker")) {
            this.dirtyTiles.forEach(access, tilePos -> executor.execute(() -> {
                int tileX = Tile.tileX(tilePos);
                int tileY = Tile.tileY(tilePos);
                try {
                    LongList elements = new LongArrayList();
                    this.tileContents.getElementsInTile(access, tilePos, elements);
                    if (elements.isEmpty()) { //nothing to write
                        return;
                    }

                    Path dir = outputRoot.resolve("tile").resolve(String.valueOf(tileX));
                    Files.createDirectories(dir);
                    try (FileChannel channel = FileChannel.open(dir.resolve(tileY + ".json"), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                        List<ByteBuffer> list = this.tempJsonStorage.getAll(access, elements);
                        list.removeIf(Objects::isNull);
                        ByteBuffer[] buffers = list.toArray(new ByteBuffer[0]);
                        int totalSize = 0;
                        for (ByteBuffer buffer : buffers) {
                            totalSize += buffer.remaining();
                        }

                        for (int i = 0; i < totalSize; i++) {
                            i += channel.write(buffers);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("tile " + tileX + ' ' + tileY, e);
                }
                notifier.step(0);
            }));
        }
    }

    public Path tileFile(@NonNull Path outputRoot, long tilePos) {
        return outputRoot.resolve(PStrings.fastFormat("tile/%d/%d.json", Tile.tileX(tilePos), Tile.tileY(tilePos)));
    }

    public void purge(boolean temp, boolean index) throws Exception {
        if (!temp && !index) { //do nothing?!?
            return;
        }

        logger.info("Cleaning up... (this might take a while)");
        this.flush();
        if (temp) {
            logger.trace("Clearing temporary GeoJSON storage...");
            this.tempJsonStorage.clear(this.db.batch());
        }
        if (index) {
            logger.trace("Clearing tile content index...");
            this.tileContents.clear(this.db.batch());
            logger.trace("Clearing geometry intersection index...");
            this.intersectedTiles.clear(this.db.batch());
            logger.trace("Clearing dirty tile markers...");
            this.dirtyTiles.clear(this.db.batch());
        }
        logger.success("Cleared.");
    }

    public void flush() throws Exception {
        this.db.flush();

        this.unprocessedElements.flush();
    }

    @Override
    public void close() throws Exception {
        this.unprocessedElements.close();

        this.replicationTimestamp.close();

        this.db.close();
    }

    public void setReplicationBaseUrl(@NonNull String baseUrl) throws Exception {
        if (!baseUrl.endsWith("/")) {
            baseUrl += '/';
        }
        this.replicationBaseUrl = baseUrl;
        Files.write(this.root.resolve("replication_base_url.txt"), baseUrl.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public ChangesetState getChangesetState() throws Exception {
        return this.getReplicationDataObject("state.txt", false, ChangesetState::new);
    }

    public ChangesetState getChangesetState(long sequence) throws Exception {
        return this.getReplicationDataObject(
                PStrings.fastFormat("%03d/%03d/%03d.state.txt", sequence / 1000000L, (sequence / 1000L) % 1000L, sequence % 1000L),
                true,
                ChangesetState::new);
    }

    public Changeset getChangeset(long sequence) throws Exception {
        return this.getReplicationDataObject(
                PStrings.fastFormat("%03d/%03d/%03d.osc.gz", sequence / 1000000L, (sequence / 1000L) % 1000L, sequence % 1000L),
                true,
                Changeset::parse);
    }

    private <T> T getReplicationDataObject(@NonNull String path, boolean cache, @NonNull IOFunction<ByteBuf, T> parser) throws IOException {
        Path file = this.root.resolve("replication").resolve(path);
        if (cache && Files.exists(file)) {
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                int size = toInt(channel.size());
                ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.ioBuffer(size, size);
                try {
                    while (buf.isWritable()) {
                        buf.writeBytes(channel, buf.writerIndex(), buf.writableBytes());
                    }
                    return parser.applyThrowing(buf);
                } finally {
                    buf.release();
                }
            }
        }

        byte[] data;
        try (InputStream in = new URL(this.replicationBaseUrl + path).openStream()) {
            data = StreamUtil.toByteArray(in);
        }

        if (cache) {
            Files.createDirectories(file.getParent());
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuf buf = Unpooled.wrappedBuffer(data);
                while (buf.isReadable()) {
                    buf.readBytes(channel, buf.readableBytes());
                }
            }
        }

        return parser.applyThrowing(Unpooled.wrappedBuffer(data));
    }
}
