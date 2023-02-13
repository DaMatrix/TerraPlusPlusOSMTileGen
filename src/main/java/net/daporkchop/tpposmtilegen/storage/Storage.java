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

package net.daporkchop.tpposmtilegen.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.oio.StreamUtil;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Line;
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
import net.daporkchop.tpposmtilegen.storage.special.ReferenceDB;
import net.daporkchop.tpposmtilegen.storage.special.TileDB;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapAtomicLong;
import org.rocksdb.Checkpoint;
import org.rocksdb.DBOptions;
import org.rocksdb.OptimisticTransactionDB;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;

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

    protected final LongArrayDB[] intersectedTiles = new LongArrayDB[MAX_LEVELS];
    protected final TileDB[] tileJsonStorage = new TileDB[MAX_LEVELS];
    protected final BlobDB[] externalJsonStorage = new BlobDB[MAX_LEVELS];

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

        Database.Builder builder = new Database.Builder()
                .autoFlush(true)
                .readOnly(readOnly)
                .add("nodes", (database, handle, descriptor) -> this.nodes = new NodeDB(database, handle, descriptor))
                .add("points", (database, handle, descriptor) -> this.points = new PointDB(database, handle, descriptor))
                .add("ways", (database, handle, descriptor) -> this.ways = new WayDB(database, handle, descriptor))
                .add("relations", (database, handle, descriptor) -> this.relations = new RelationDB(database, handle, descriptor))
                .add("coastlines", (database, handle, descriptor) -> this.coastlines = new CoastlineDB(database, handle, descriptor))
                .add("references", (database, handle, descriptor) -> this.references = new ReferenceDB(database, handle, descriptor))
                .add("sequence_number", (database, handle, descriptor) -> this.sequenceNumber = new DBLong(database, handle, descriptor));
        IntStream.range(0, MAX_LEVELS).forEach(lvl -> builder.add("intersected_tiles@" + lvl, (database, handle, descriptor) -> this.intersectedTiles[lvl] = new LongArrayDB(database, handle, descriptor)));
        IntStream.range(0, MAX_LEVELS).forEach(lvl -> builder.add("tiles@" + lvl, Database.COLUMN_OPTIONS_COMPACT, (database, handle, descriptor) -> this.tileJsonStorage[lvl] = new TileDB(database, handle, descriptor)));
        IntStream.range(0, MAX_LEVELS).forEach(lvl -> builder.add("external_json@" + lvl, Database.COLUMN_OPTIONS_COMPACT, (database, handle, descriptor) -> this.externalJsonStorage[lvl] = new BlobDB(database, handle, descriptor)));
        try (TimedOperation operation = new TimedOperation("Open DB")) {
            this.db = builder.build(root.resolve("db"), options);
        }

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

    public void convertToGeoJSONAndStoreInDB(@NonNull DBAccess access, long combinedId, Element element, boolean allowUnknown) throws Exception {
        int type = Element.extractType(combinedId);
        String typeName = Element.typeName(type);
        long id = Element.extractId(combinedId);

        if (element == null) { //element isn't provided, attempt to load it
            element = this.getElement(access, combinedId);
        }
        if (element == null && !allowUnknown) {
            throw new IllegalStateException(PStrings.fastFormat("unknown %s %d", typeName, id));
        }

        String location = Geometry.externalStorageLocation(type, id);

        //pass 1: delete element from everything
        for (int lvl = 0; lvl < MAX_LEVELS; lvl++) {
            long[] oldIntersected = this.intersectedTiles[lvl].get(access, combinedId);
            if (oldIntersected != null) { //the element previously existed at this level, delete it
                //element should no longer intersect any tiles
                this.intersectedTiles[lvl].delete(access, combinedId);

                //remove the element's json data from all tiles it previously intersected
                this.tileJsonStorage[lvl].deleteElementFromTiles(access, LongArrayList.wrap(oldIntersected), combinedId);

                //delete element's external json data, if it was present
                this.externalJsonStorage[lvl].delete(access, combinedId);
            } else { //if the element wasn't present at this level, it won't be at any other levels either
                break;
            }
        }

        //pass 2: add element to all its new destination tiles, if it exists
        Geometry geometry;
        if (element != null && (geometry = element.toGeometry(this, access)) != null) {
            int lvl = 0;
            for (Geometry simplifiedGeometry; lvl < MAX_LEVELS && (simplifiedGeometry = geometry.simplifyTo(lvl)) != null; lvl++) {
                long[] intersected = simplifiedGeometry.listIntersectedTiles(lvl);
                Arrays.sort(intersected);

                this.intersectedTiles[lvl].put(access, combinedId, intersected);

                //encode geometry to GeoJSON
                ByteBuf buffer;
                try (Handle<StringBuilder> handle = PorkUtil.STRINGBUILDER_POOL.get()) {
                    StringBuilder builder = handle.get();
                    builder.setLength(0);

                    Geometry.toGeoJSON(builder, simplifiedGeometry, element.tags(), combinedId);

                    //convert json to bytes
                    buffer = Geometry.toByteBuf(builder);
                }

                try {
                    if (!simplifiedGeometry.shouldStoreExternally(intersected.length, buffer.readableBytes())) {
                        //the element's geometry is small enough that storing it in multiple tiles should be a non-issue
                        this.tileJsonStorage[lvl].addElementToTiles(access, LongArrayList.wrap(intersected), combinedId, buffer);
                    } else {
                        //element is referenced multiple times, store it in an external file
                        this.externalJsonStorage[lvl].put(access, combinedId, buffer.nioBuffer());

                        ByteBuf referenceBuffer = Geometry.createReference(location);
                        try {
                            this.tileJsonStorage[lvl].addElementToTiles(access, LongArrayList.wrap(intersected), combinedId, referenceBuffer);
                        } finally {
                            referenceBuffer.release();
                        }
                    }
                } finally {
                    buffer.release();
                }
            }
        }
    }

    public ByteBuf getTile(@NonNull DBAccess access, int tileX, int tileY, int level) throws Exception {
        long tilePos = Tile.xy2tilePos(tileX, tileY);

        //TODO: make this use a FeatureCollection again

        ByteBuf merged = UnpooledByteBufAllocator.DEFAULT.ioBuffer()
                .writeBytes(Geometry._FEATURECOLLECTION_PREFIX);
        merged.clear();

        int startWriterIndex = merged.writerIndex();

        this.tileJsonStorage[level].getElementsInTile(access, tilePos, (combinedId, json) -> {
            checkArg(json.length != 0, "empty json data for %s %d", Element.typeName(Element.extractType(combinedId)), Element.extractId(combinedId));
            //merged.writeBytes(json).writeByte(',');
            merged.writeBytes(json);
            if (merged.getByte(merged.writerIndex() - 1) != '\n') {
                merged.writeByte('\n');
            }
        });

        if (merged.writerIndex() != startWriterIndex) {
            merged.writerIndex(merged.writerIndex() - 1);
        }
        //merged.writeBytes(Geometry._FEATURECOLLECTION_SUFFIX);

        return merged;
    }

    public void purge(boolean full) throws Exception {
        logger.info("Cleaning up... (this might take a while)");
        this.flush();

        if (full) {
            logger.trace("Clearing geometry intersection index...");
            for (LongArrayDB db : this.intersectedTiles) {
                db.clear();
            }
            logger.trace("Clearing per-tile GeoJSON storage...");
            for (TileDB db : this.tileJsonStorage) {
                db.clear();
            }
            logger.trace("Clearing external GeoJSON storage...");
            for (BlobDB db : this.externalJsonStorage) {
                db.clear();
            }
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

        logger.info("Copying extra stuff...");
        Files.write(tmpDir.resolve("replication_base_url.txt"), this.replicationBaseUrl.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        new OffHeapAtomicLong(tmpDir.resolve("osm_replicationTimestamp"), this.replicationTimestamp.get()).close();

        if (Files.exists(this.root.resolve("custom"))) {
            this.copyRecursive(this.root.resolve("custom"), tmpDir.resolve("custom"));
        }

        logger.info("Finishing up...");
        Files.move(tmpDir, dst);
    }

    private void copyRecursive(@NonNull Path src, @NonNull Path dst) throws IOException {
        if (Files.isDirectory(src)) {
            try (Stream<Path> stream = Files.list(src)) {
                stream.forEach((IOConsumer<Path>) path -> this.copyRecursive(path, dst.resolve(path.getFileName())));
            }
        } else {
            Files.copy(src, dst);
        }
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
