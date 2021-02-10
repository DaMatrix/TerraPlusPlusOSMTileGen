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
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.util.PorkUtil;
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
import net.daporkchop.tpposmtilegen.storage.special.StringToBlobDB;
import net.daporkchop.tpposmtilegen.storage.special.TileDB;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapAtomicLong;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.OptimisticTransactionDB;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongConsumer;

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

    protected DBLong sequenceNumber;
    protected final OffHeapAtomicLong replicationTimestamp;
    protected String replicationBaseUrl;

    protected ReferenceDB references;
    protected TileDB tileContents;
    protected DirtyTracker dirtyTiles;
    protected LongArrayDB intersectedTiles;

    protected BlobDB jsonStorage;
    protected StringToBlobDB files;

    protected final Database db;

    protected final Path root;

    public Storage(@NonNull Path root) throws Exception {
        this(root, Database.DB_OPTIONS);
    }

    public Storage(@NonNull Path root, @NonNull DBOptions options) throws Exception {
        this(root, options, false);
    }

    public Storage(@NonNull Path root, @NonNull DBOptions options, boolean readOnly) throws Exception {
        this.root = root;

        this.db = new Database.Builder()
                .add("nodes", (database, handle, descriptor) -> this.nodes = new NodeDB(database, handle, descriptor))
                .add("points", (database, handle, descriptor) -> this.points = new PointDB(database, handle, descriptor))
                .add("ways", (database, handle, descriptor) -> this.ways = new WayDB(database, handle, descriptor))
                .add("relations", (database, handle, descriptor) -> this.relations = new RelationDB(database, handle, descriptor))
                .add("coastlines", (database, handle, descriptor) -> this.coastlines = new CoastlineDB(database, handle, descriptor))
                .add("references", (database, handle, descriptor) -> this.references = new ReferenceDB(database, handle, descriptor))
                .add("tiles", (database, handle, descriptor) -> this.tileContents = new TileDB(database, handle, descriptor))
                .add("intersected_tiles", (database, handle, descriptor) -> this.intersectedTiles = new LongArrayDB(database, handle, descriptor))
                .add("dirty_tiles", CompressionType.NO_COMPRESSION, (database, handle, descriptor) -> this.dirtyTiles = new DirtyTracker(database, handle, descriptor))
                .add("json", CompressionType.ZSTD_COMPRESSION, (database, handle, descriptor) -> this.jsonStorage = new BlobDB(database, handle, descriptor))
                .add("files", CompressionType.ZSTD_COMPRESSION, (database, handle, descriptor) -> this.files = new StringToBlobDB(database, handle, descriptor))
                .add("sequence_number", (database, handle, descriptor) -> this.sequenceNumber = new DBLong(database, handle, descriptor))
                .autoFlush(true)
                .readOnly(readOnly)
                .build(root.resolve("db"), options);

        this.replicationTimestamp = new OffHeapAtomicLong(root.resolve("osm_replicationTimestamp"), -1L);

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
        }
    }

    public void putWay(@NonNull DBAccess access, @NonNull Way way) throws Exception {
        this.ways.put(access, way.id(), way);
    }

    public void putRelation(@NonNull DBAccess access, @NonNull Relation relation) throws Exception {
        this.relations.put(access, relation.id(), relation);
    }

    public Element getElement(@NonNull DBAccess access, long combinedId) throws Exception {
        int type = Element.extractType(combinedId);
        long id = Element.extractId(combinedId);
        RocksDBMap<? extends Element> map = this.elementsByType.getOrDefault(type, null);
        checkArg(map != null, "unknown element type %d (id %d)", type, id);
        return map.get(access, id);
    }

    public void convertToGeoJSONAndStoreInDB(@NonNull DBAccess access, long combinedId, boolean allowUnknown) throws Exception {
        int type = Element.extractType(combinedId);
        String typeName = Element.typeName(type);
        long id = Element.extractId(combinedId);

        String location = Geometry.externalStorageLocation(type, id);
        long[] oldIntersected = this.intersectedTiles.get(access, combinedId);

        Element element = this.getElement(access, combinedId);
        if (element == null && !allowUnknown) {
            throw new IllegalStateException(PStrings.fastFormat("unknown %s %d", typeName, id));
        }

        Geometry geometry;
        if (element != null && (geometry = element.toGeometry(this, access)) != null) {
            long[] arr = geometry.listIntersectedTiles();
            Arrays.sort(arr);
            int tileCount = arr.length;

            this.dirtyTiles.markDirty(access, LongArrayList.wrap(arr));
            this.tileContents.addElementToTiles(access, LongArrayList.wrap(arr), combinedId); //add this element to all tiles
            this.intersectedTiles.put(access, combinedId, arr);

            //encode geometry to GeoJSON
            ByteBuffer buffer;
            try (Handle<StringBuilder> handle = PorkUtil.STRINGBUILDER_POOL.get()) {
                StringBuilder builder = handle.get();
                builder.setLength(0);

                Geometry.toGeoJSON(builder, geometry, element.tags(), combinedId);

                //convert json to bytes
                buffer = Geometry.toBytes(builder);
            }

            try {
                if (!geometry.shouldStoreExternally(tileCount, buffer.remaining())) {
                    //the element's geometry is small enough that storing it in multiple tiles should be a non-issue
                    this.jsonStorage.put(access, combinedId, buffer);
                    this.files.delete(access, "0/" + location);
                } else { //element is referenced multiple times, store it in an external file
                    this.files.put(access, "0/" + location, buffer);

                    ByteBuffer referenceBuffer = Geometry.createReference(location);
                    this.jsonStorage.put(access, combinedId, referenceBuffer);
                    PUnsafe.pork_releaseBuffer(referenceBuffer);
                }
            } finally {
                PUnsafe.pork_releaseBuffer(buffer);
            }
        } else {
            //delete element from everything
            if (oldIntersected != null) {
                this.intersectedTiles.delete(access, combinedId);
                this.tileContents.deleteElementFromTiles(access, LongArrayList.wrap(oldIntersected), combinedId);
            }
            this.files.delete(access, location);
        }
    }

    public void exportDirtyTiles(@NonNull DBAccess access) throws Exception {
        long count = this.dirtyTiles.count(access);

        try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Write dirty tiles")
                .slot("tiles", count)
                .build()) {
            if (access.threadSafe()) {
                notifier.logger().debug("using parallel mode");
                this.dirtyTiles.forEachParallel(access, this.exportTile(access, notifier));
            } else {
                notifier.logger().debug("using single-threaded mode");
                this.dirtyTiles.forEach(access, this.exportTile(access, notifier));
            }
        }
    }

    protected LongConsumer exportTile(@NonNull DBAccess access, @NonNull ProgressNotifier notifier) {
        return tilePos -> {
            String path = PStrings.fastFormat("0/tile/%d/%d.json", Tile.tileX(tilePos), Tile.tileY(tilePos));

            try {
                LongList elements = new LongArrayList();
                this.tileContents.getElementsInTile(access, tilePos, elements);

                List<ByteBuffer> list = this.jsonStorage.getAll(access, elements);

                //compute total size
                int count = list.size();
                int size = list.stream().mapToInt(ByteBuffer::remaining).sum();

                byte[] merged = null;
                if (count != 0) { //merge buffers
                    merged = new byte[size];
                    for (int i = 0, off = 0; i < count; i++) {
                        ByteBuffer buf = list.get(i);
                        list.set(i, null);
                        int remaining = buf.remaining();
                        buf.get(merged, off, remaining);
                        off += remaining;
                    }
                }

                if (merged != null) {
                    this.files.putHeap(access, path, ByteBuffer.wrap(merged));
                } else {
                    this.files.delete(access, path);
                }
                this.dirtyTiles.unmarkDirty(access, tilePos);
            } catch (Exception e) {
                throw new RuntimeException("tile " + Tile.tileX(tilePos) + ' ' + Tile.tileY(tilePos), e);
            }
            notifier.step(0);
        };
    }

    public void purge(boolean full) throws Exception {
        logger.info("Cleaning up... (this might take a while)");
        this.flush();

        logger.trace("Clearing dirty tile markers...");
        this.dirtyTiles.clear();

        if (full) {
            logger.trace("Clearing tile assembly GeoJSON storage...");
            this.jsonStorage.clear();
            logger.trace("Clearing completed file storage...");
            this.files.clear();
            logger.trace("Clearing tile content index...");
            this.tileContents.clear();
            logger.trace("Clearing geometry intersection index...");
            this.intersectedTiles.clear();
        }

        logger.success("Cleared.");
    }

    public void flush() throws Exception {
        this.db.flush();
    }

    public void createSnapshot(@NonNull Path dst) throws Exception {
        Path tmpDir = Files.createDirectories(dst.resolveSibling(dst.getFileName().toString() + ".tmp"));

        logger.info("Constructing snapshot...");
        try (Checkpoint checkpoint = Checkpoint.create(this.db.delegate())) {
            logger.info("Creating snapshot...");
            checkpoint.createCheckpoint(tmpDir.resolve("db").toString());
        }

        logger.info("Writing extra attributes...");
        Files.write(tmpDir.resolve("replication_base_url.txt"), this.replicationBaseUrl.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        new OffHeapAtomicLong(tmpDir.resolve("osm_replicationTimestamp"), this.replicationTimestamp.get()).close();

        logger.info("Finishing up...");
        Files.move(tmpDir, dst);
    }

    @Override
    public void close() throws Exception {
        if (this.db.delegate() instanceof OptimisticTransactionDB) {
            this.flush();

            this.db.delegate().flushWal(true);
            this.db.delegate().flush(Database.FLUSH_OPTIONS);
            this.db.delegate().flushWal(true);
        }

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
