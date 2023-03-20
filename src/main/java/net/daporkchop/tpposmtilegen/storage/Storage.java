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
import io.netty.buffer.UnpooledByteBufAllocator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.oio.StreamUtil;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.function.io.IOUnaryOperator;
import net.daporkchop.lib.common.function.exception.ESupplier;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.refcount.AbstractRefCounted;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.pool.recycler.Recycler;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.common.util.exception.AlreadyReleasedException;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.natives.DBPropertiesMergeOperator;
import net.daporkchop.tpposmtilegen.natives.UInt64SetMergeOperator;
import net.daporkchop.tpposmtilegen.natives.UInt64ToBlobMapMergeOperator;
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
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.storage.special.DBLong;
import net.daporkchop.tpposmtilegen.storage.special.DBProperties;
import net.daporkchop.tpposmtilegen.storage.special.ReferenceDB;
import net.daporkchop.tpposmtilegen.storage.special.TileDB;
import net.daporkchop.tpposmtilegen.util.Tile;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import net.daporkchop.tpposmtilegen.util.Utils;
import org.rocksdb.Checkpoint;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
    public static final String DEFAULT_REPLICATION_BASE_URL = System.getProperty("defaultReplicationBaseUrl", "https://planet.openstreetmap.org/replication/minute/");

    private static final int REPLICATION_BLOBS_CACHE_CLEANUP_THRESHOLD = 128;
    private static final int REPLICATION_BLOBS_CACHE_CLEANUP_TARGET = 64;
    private static final long REPLICATION_BLOBS_CACHE_PREFETCH_DISTANCE = 1L;

    protected NodeDB nodes;
    protected PointDB points;
    protected WayDB ways;
    protected RelationDB relations;
    protected CoastlineDB coastlines;
    protected final Int2ObjectMap<RocksDBMap<? extends Element>> elementsByType = new Int2ObjectOpenHashMap<>();

    protected DBProperties properties;
    protected DBProperties.LongProperty versionNumberProperty;
    protected DBProperties.LongProperty sequenceNumberProperty;
    protected DBProperties.LongProperty replicationTimestampProperty;
    protected DBProperties.StringProperty replicationBaseUrlProperty;

    @Deprecated
    protected DBLong sequenceNumber;

    protected ReferenceDB references;

    //TODO: index these properly with MIN_LEVEL
    protected final LongArrayDB[] intersectedTiles = new LongArrayDB[MAX_LEVEL];
    protected final TileDB[] tileJsonStorage = new TileDB[MAX_LEVEL];
    protected final BlobDB[] externalJsonStorage = new BlobDB[MAX_LEVEL];

    protected final Database db;

    protected final Path root;

    protected final Path tmpDirectoryPath;
    protected final Set<Path> activeTmpFiles = new TreeSet<>();

    protected final Path replicationDirectoryPath;
    protected final Object2ObjectLinkedOpenHashMap<String, CompletableFuture<byte[]>> replicationBlobs
            = new Object2ObjectLinkedOpenHashMap<>(REPLICATION_BLOBS_CACHE_CLEANUP_THRESHOLD);

    public final boolean legacy;

    public Storage(@NonNull Path root) throws Exception {
        this(root, DatabaseConfig.RW_GENERAL);
    }

    public Storage(@NonNull Path root, @NonNull DatabaseConfig config) throws Exception {
        this.root = root;

        boolean legacy;
        if ("/media/daporkchop/data/planet-4-aggressive-zstd-compression/planet".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-0".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-2".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-3".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v3-compact-everything-constantly".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v2-compact-tiles-after-post-compaction".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v3-updated-5456500-original-slow".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v3-updated-5456500-new-technique-2-very-fast".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v3-updated-to-5466051-v2".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v3-recomputed-references-updated-to-5466051-v3".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-v3-recomputed-references-updated-to-5466051-v3-removed-empty-references".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227-v2".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227-v2-with-changeset-updated-to-5466051".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227-v2-with-changeset-updated-to-5466051-with-coastlines".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227-v2-with-changeset-updated-to-5466051-with-coastlines-v2".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227-v2-with-changeset-updated-to-5466051-with-coastlines-v2-recomputed-references".equals(root.toString())
            || "/media/daporkchop/data/planet-test/planet-assembled-230227-v1-with-changeset-updated-to-5466051".equals(root.toString())
            || "/media/daporkchop/data/switzerland".equals(root.toString())
            || "/media/daporkchop/data/switzerland-reference".equals(root.toString())
            || "/mnt/planet".equals(root.toString())) {
            legacy = false;
        } else if ("/media/daporkchop/data/planet-5-dictionary-zstd-compression/planet".equals(root.toString())
                   || "/media/daporkchop/data/planet-legacy-references/planet".equals(root.toString())
                   || "/media/daporkchop/data/switzerland-legacy".equals(root.toString())) {
            legacy = true;

            logger.alert("storage at '%s' being opened in legacy mode!", root);
        } else {
            throw new IllegalArgumentException(root.toString());
        }
        this.legacy = legacy;

        if (!config.readOnly() && PFiles.checkFileExists(root.resolve("db").resolve("IDENTITY"))) {
            //if we're trying to open the storage read-write, we should first open and close it read-only in order to double-check the version number without breaking
            // anything
            try (Storage readOnlyStorage = new Storage(root, DatabaseConfig.RO_LITE)) {
                //no-op
            }
        }

        Database.Builder builder = new Database.Builder(config)
                .autoFlush(true)
                //.add("properties", (database, handle, descriptor) -> this.properties = new DBProperties(database, handle, descriptor))
                .add("nodes", (database, handle, descriptor) -> this.nodes = new NodeDB(database, handle, descriptor))
                .add("points", (database, handle, descriptor) -> this.points = new PointDB(database, handle, descriptor))
                .add("ways", (database, handle, descriptor) -> this.ways = new WayDB(database, handle, descriptor))
                .add("relations", (database, handle, descriptor) -> this.relations = new RelationDB(database, handle, descriptor))
                .add("coastlines", (database, handle, descriptor) -> this.coastlines = new CoastlineDB(database, handle, descriptor))
                .add("references", (database, handle, descriptor) -> this.references = legacy ? new ReferenceDB.Legacy(database, handle, descriptor) : new ReferenceDB(database, handle, descriptor), legacy ? null : UInt64SetMergeOperator.INSTANCE);

        if (legacy && !"/media/daporkchop/data/switzerland-legacy".equals(root.toString())) {
            builder.add("sequence_number", (database, handle, descriptor) -> this.sequenceNumber = new DBLong(database, handle, descriptor));
        } else {
            builder.add("properties", (database, handle, descriptor) -> this.properties = new DBProperties(database, handle, descriptor), DBPropertiesMergeOperator.UINT64_ADD_OPERATOR);
        }

        IntStream.range(MIN_LEVEL, MAX_LEVEL).forEach(lvl -> builder.add("intersected_tiles@" + lvl, (database, handle, descriptor) -> this.intersectedTiles[lvl] = new LongArrayDB(database, handle, descriptor)));
        IntStream.range(MIN_LEVEL, MAX_LEVEL).forEach(lvl -> builder.add("tiles@" + lvl, DatabaseConfig.ColumnFamilyType.COMPACT, (database, handle, descriptor) -> this.tileJsonStorage[lvl] = legacy ? new TileDB.Legacy(database, handle, descriptor) : new TileDB(database, handle, descriptor), legacy ? null : UInt64ToBlobMapMergeOperator.INSTANCE));
        IntStream.range(MIN_LEVEL, MAX_LEVEL).forEach(lvl -> builder.add("external_json@" + lvl, DatabaseConfig.ColumnFamilyType.COMPACT, (database, handle, descriptor) -> this.externalJsonStorage[lvl] = new BlobDB(database, handle, descriptor)));
        try (TimedOperation operation = new TimedOperation("Open DB")) {
            this.db = builder.build(root.resolve("db"));
        }

        if (!legacy || "/media/daporkchop/data/switzerland-legacy".equals(root.toString())) {
            this.versionNumberProperty = this.properties.getLongProperty("versionNumber");
            this.sequenceNumberProperty = this.properties.getLongProperty("sequenceNumber");
            this.replicationTimestampProperty = this.properties.getLongProperty("replicationTimestamp");
            this.replicationBaseUrlProperty = this.properties.getStringProperty("replicationBaseUrl");
        }

        this.elementsByType.put(Node.TYPE, this.nodes);
        this.elementsByType.put(Way.TYPE, this.ways);
        this.elementsByType.put(Relation.TYPE, this.relations);
        this.elementsByType.put(Coastline.TYPE, this.coastlines);

        this.tmpDirectoryPath = root.resolve("tmp");
        this.replicationDirectoryPath = root.resolve("replication");

        if (!legacy) {
            OptionalLong version = this.versionNumberProperty.getLong(this.db.read());
            final long supportedVersion = 2L;
            if (!version.isPresent()) {
                if (!config.readOnly()) {
                    try (DBWriteAccess batch = this.db.beginLocalBatch()) {
                        this.versionNumberProperty.set(batch, supportedVersion);
                    }
                }
            } else if (version.getAsLong() != supportedVersion) {
                throw new IllegalStateException("storage at '" + root + "' is at version v" + version.getAsLong() + ", but this version of T++OSMTileGen only supports v" + supportedVersion);
            }
        }
    }

    public void putNode(@NonNull DBWriteAccess access, @NonNull Node node, @NonNull Point point) throws Exception {
        this.points.put(access, node.id(), point);
        this.nodes.put(access, node.id(), node);
    }

    public void deleteNode(@NonNull DBWriteAccess access, long id) throws Exception {
        this.points.delete(access, id);
        this.nodes.delete(access, id);
    }

    public void putWay(@NonNull DBWriteAccess access, @NonNull Way way) throws Exception {
        this.ways.put(access, way.id(), way);
    }

    public void deleteWay(@NonNull DBWriteAccess access, long id) throws Exception {
        this.ways.delete(access, id);
    }

    public void putRelation(@NonNull DBWriteAccess access, @NonNull Relation relation) throws Exception {
        this.relations.put(access, relation.id(), relation);
    }

    public void deleteRelation(@NonNull DBWriteAccess access, long id) throws Exception {
        this.relations.delete(access, id);
    }

    public Element getElement(@NonNull DBReadAccess access, long combinedId) throws Exception {
        int type = Element.extractType(combinedId);
        long id = Element.extractId(combinedId);
        RocksDBMap<? extends Element> map = this.elementsByType.getOrDefault(type, null);
        checkArg(map != null, "unknown element type %d (id %d)", type, id);
        return map.get(access, id);
    }

    public void removeGeoJSONFromDB(@NonNull DBAccess access, long combinedId) throws Exception {
        for (int lvl = 0; lvl < MAX_LEVEL; lvl++) {
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
    }

    /**
     * Assembles the given element to GeoJSON geometry and adds the result to every intersected tile at every level.
     *
     * @param currentStateReadAccess a {@link DBReadAccess} for reading the current database state. This is expected to contain the newest data for all element types.
     *                               For all tiles/external blobs/intersected tiles, this must initially contain the old data, however it is guaranteed that none of these
     *                               data types will be read from once written to in {@code nextStateWriteAccess}, so it is not defined whether writes will be made
     *                               visible immediately (this allows the same transaction to be used for both {@code currentStateReadAccess} and
     *                               {@code nextStateWriteAccess}, or for {@code currentStateReadAccess} to refer to the live database contents/a snapshot while
     *                               {@code nextStateWriteAccess} is a write batch, etc.)
     * @param nextStateWriteAccess   a {@link DBWriteAccess} for writing tiles/external blobs/intersected tiles data
     * @param oldElement             the old element instance (may be {@code null} if the element was not previously assembled)
     * @param newElement             the new element instance
     */
    public void assembleElement(@NonNull DBReadAccess currentStateReadAccess, @NonNull DBWriteAccess nextStateWriteAccess, Element oldElement, @NonNull Element newElement) throws Exception {
        int type = newElement.type();
        long id = newElement.id();
        long combinedId = Element.addTypeToId(type, id);

        if (oldElement != null) {
            checkArg(oldElement.type() == type && oldElement.id() == id);
        }

        //add element to all its new destination tiles, if it exists
        Geometry newGeometry = newElement.allowedToIncludeAtLevel(MIN_LEVEL) ? newElement.toGeometry(this, currentStateReadAccess) : null;

        boolean anyNewLevelWasNull = newGeometry == null || !newElement.visible();
        boolean anyOldLevelWasNull = oldElement == null || !oldElement.visible();

        for (int lvl = MIN_LEVEL; !(anyOldLevelWasNull && anyNewLevelWasNull) && lvl < MAX_LEVEL; lvl++) {
            Geometry simplifiedGeometry = !anyNewLevelWasNull && newElement.allowedToIncludeAtLevel(lvl) ? newGeometry.simplifyTo(lvl).orElse(null) : null;
            if (simplifiedGeometry == null) {
                anyNewLevelWasNull = true;
            }

            long[] newIntersected = simplifiedGeometry != null ? Objects.requireNonNull(simplifiedGeometry.listIntersectedTiles(lvl)) : null;
            if (newIntersected != null) {
                Utils.maybeParallelSort(newIntersected);
            }

            long[] oldIntersected = !anyOldLevelWasNull ? this.intersectedTiles[lvl].get(currentStateReadAccess, combinedId) : null;
            if (oldIntersected != null) {
                Utils.maybeParallelSort(oldIntersected);
            } else {
                anyOldLevelWasNull = true;
            }

            if (newIntersected != null) { //the element currently exists at this level, store its intersected tiles list (potentially overwriting the old one)
                if (Arrays.equals(newIntersected, oldIntersected)) {
                    //we can avoid overwriting the old intersected tiles list if it hasn't changed
                } else {
                    this.intersectedTiles[lvl].put(nextStateWriteAccess, combinedId, newIntersected);
                }
            } else if (oldIntersected != null) { //the element used to exist at this level, but no longer does!
                //delete the element's old intersected tiles list completely

                this.intersectedTiles[lvl].delete(nextStateWriteAccess, combinedId);
            }

            //encode geometry to GeoJSON
            ByteBuf newTileBuffer = null;
            ByteBuf newExternalBuffer = null;
            try {
                if (simplifiedGeometry != null) {
                    Recycler<StringBuilder> stringBuilderRecycler = PorkUtil.stringBuilderRecycler();
                    StringBuilder builder = stringBuilderRecycler.allocate();

                    Geometry.toGeoJSON(builder, simplifiedGeometry, newElement.tags(), combinedId);

                    //convert json to bytes
                    newTileBuffer = Geometry.toByteBuf(builder);
                    stringBuilderRecycler.release(builder);

                    if (simplifiedGeometry.shouldStoreExternally(newIntersected.length, newTileBuffer.readableBytes())) {
                        //we can't store the element's geometry inline in the tile data, store a reference to it in the tile and add the actual geometry to the external json
                        newExternalBuffer = newTileBuffer;
                        newTileBuffer = Geometry.createReference(type, id);
                    }
                }

                boolean canReuseExistingTileJson = false;

                if (newExternalBuffer != null) { //the element needs to have an external json blob at this level, store it (potentially overwriting the old one)
                    if (oldIntersected != null) { //the element was already assembled before
                        //if there's already an existing external json blob for this element, we know that the old version of the element was being stored externally
                        // and therefore each tile that it intersected contains a reference to the external blob. as the element's ID hasn't changed, this reference is
                        // still valid for the new version of the element and may be re-used!
                        canReuseExistingTileJson = this.externalJsonStorage[lvl].contains(currentStateReadAccess, combinedId);
                    }

                    this.externalJsonStorage[lvl].put(nextStateWriteAccess, combinedId, newExternalBuffer.nioBuffer());
                } else if (oldIntersected != null) { //the element used to exist at this level, but no longer does!
                    //delete its external json blob, if it was present
                    this.externalJsonStorage[lvl].delete(nextStateWriteAccess, combinedId);
                }

                if (newTileBuffer != null) { //the element currently has json data at this level, store it into each intersected tile (potentially overwriting any old ones)
                    if (canReuseExistingTileJson) {
                        //we don't have to add the element's json data to every newly intersected tile, only the ones which weren't intersected before!
                        long[] toAddTo = Utils.maybeParallelStream(newIntersected)
                                .filter(newIntersectedValue -> Arrays.binarySearch(oldIntersected, newIntersectedValue) < 0) //filter to only include values that aren't in oldIntersected
                                .toArray();

                        this.tileJsonStorage[lvl].addElementToTiles(nextStateWriteAccess, LongArrayList.wrap(toAddTo), combinedId, newTileBuffer);
                    } else {
                        //simply add the element's json data to each newly intersected tile
                        this.tileJsonStorage[lvl].addElementToTiles(nextStateWriteAccess, LongArrayList.wrap(newIntersected), combinedId, newTileBuffer);
                    }

                    if (oldIntersected != null) { //the element used to exist at this level, delete it from every tile which it no longer intersects
                        long[] toRemoveFrom = Utils.maybeParallelStream(oldIntersected)
                                .filter(oldIntersectedValue -> Arrays.binarySearch(newIntersected, oldIntersectedValue) < 0) //filter to only include values that aren't in newIntersected
                                .toArray();

                        this.tileJsonStorage[lvl].deleteElementFromTiles(nextStateWriteAccess, LongArrayList.wrap(toRemoveFrom), combinedId);
                    }
                } else if (oldIntersected != null) { //the element used to exist at this level, but no longer does!
                    //delete the element from every tile it used to be stored in
                    this.tileJsonStorage[lvl].deleteElementFromTiles(nextStateWriteAccess, LongArrayList.wrap(oldIntersected), combinedId);
                }
            } finally {
                if (newExternalBuffer != null) {
                    newExternalBuffer.release();
                }
                if (newTileBuffer != null) {
                    newTileBuffer.release();
                }
            }
        }
    }

    public ByteBuf getTile(@NonNull DBReadAccess access, int tileX, int tileY, int level) throws Exception {
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
        if (!this.db.config().readOnly()) {
            this.flush();

            this.db.delegate().flushWal(true);
            this.db.delegate().flush(this.db.config().flushOptions(DatabaseConfig.FlushType.GENERAL));
            this.db.delegate().flushWal(true);
        }

        this.db.close();

        if (!this.activeTmpFiles.isEmpty()) {
            logger.alert("some temporary files have not been closed:\n\n" + this.activeTmpFiles);
        }
        PFiles.rm(this.tmpDirectoryPath);
    }

    public CompletableFuture<ChangesetState> getLatestChangesetState() throws Exception {
        return this.requestReplicationDataObject("state.txt", false).thenApply(ChangesetState::new);
    }

    public CompletableFuture<ChangesetState> getChangesetState(long sequence, ChangesetState latestChangesetState) throws Exception {
        CompletableFuture<ChangesetState> future = this.requestReplicationDataObject(this.formatChangesetPathState(sequence), true).thenApply(ChangesetState::new);
        this.prefetchChangeset(sequence, latestChangesetState);
        return future;
    }

    public CompletableFuture<Changeset> getChangeset(long sequence, ChangesetState latestChangesetState) throws Exception {
        CompletableFuture<Changeset> future = this.requestReplicationDataObject(this.formatChangesetPathData(sequence), true)
                .thenApply((IOFunction<byte[], Changeset>) Changeset::parse);
        this.prefetchChangeset(sequence, latestChangesetState);
        return future;
    }

    private void prefetchChangeset(long sequence, ChangesetState latestChangesetState) throws Exception {
        if (latestChangesetState != null) {
            for (long d = 1L; d <= REPLICATION_BLOBS_CACHE_PREFETCH_DISTANCE && sequence + d <= latestChangesetState.sequenceNumber(); d++) {
                //request both the state and the data
                this.requestReplicationDataObject(this.formatChangesetPathState(sequence + d), true);
                this.requestReplicationDataObject(this.formatChangesetPathData(sequence + d), true);
            }
        }
    }

    private String formatChangesetPathState(long sequence) {
        return this.formatChangesetPath(sequence, "state.txt");
    }

    private String formatChangesetPathData(long sequence) {
        return this.formatChangesetPath(sequence, "osc.gz");
    }

    private String formatChangesetPath(long sequence, @NonNull String extension) {
        return PStrings.fastFormat("%03d/%03d/%03d.%s", sequence / 1000000L, (sequence / 1000L) % 1000L, sequence % 1000L, extension);
    }

    private void cleanupReplicationDataObjectCache() {
        if (this.replicationBlobs.size() > REPLICATION_BLOBS_CACHE_CLEANUP_THRESHOLD) {
            synchronized (this.replicationBlobs) {
                if (this.replicationBlobs.size() > REPLICATION_BLOBS_CACHE_CLEANUP_THRESHOLD) {
                    int cleaned = 0;
                    logger.trace("beginning replication URL cache cleanup...");
                    try {
                        do {
                            if (!this.replicationBlobs.get(this.replicationBlobs.lastKey()).isDone()) {
                                logger.warn("aborting cache cleanup because URL '%s' has not finished downloading", this.replicationBlobs.lastKey());
                                break;
                            }
                            this.replicationBlobs.removeLast();
                        } while (this.replicationBlobs.size() > REPLICATION_BLOBS_CACHE_CLEANUP_TARGET);
                    } finally {
                        logger.trace("finished replication URL cache cleanup, removed %d entries", cleaned);
                    }
                }
            }
        }
    }

    private CompletableFuture<byte[]> requestReplicationDataObject(@NonNull String path, boolean cache) throws Exception {
        if (cache) {
            synchronized (this.replicationBlobs) {
                CompletableFuture<byte[]> future = this.replicationBlobs.getAndMoveToFirst(path);
                if (future != null) {
                    return future;
                }
            }
        }

        CompletableFuture<byte[]> future = CompletableFuture.supplyAsync((ESupplier<CompletableFuture<byte[]>>) () -> {
            Path file = this.replicationDirectoryPath.resolve(path);
            if (cache && Files.exists(file)) { //file is already cached
                return CompletableFuture.completedFuture(Files.readAllBytes(file));
            }

            Optional<String> replicationBaseUrl = this.replicationBaseUrlProperty().get(this.db().read());
            if (!replicationBaseUrl.isPresent()) {
                logger.warn("no replication base url is stored, falling back to default: '%s'", DEFAULT_REPLICATION_BASE_URL);
                if (!this.db().config().readOnly()) {
                    try (DBWriteAccess batch = this.db().beginLocalBatch()) {
                        this.replicationBaseUrlProperty().set(batch, DEFAULT_REPLICATION_BASE_URL);
                    }
                }
                replicationBaseUrl = Optional.of(DEFAULT_REPLICATION_BASE_URL);
            }

            return this.downloadUrl(replicationBaseUrl.get() + path)
                    .thenApplyAsync((IOUnaryOperator<byte[]>) data -> {
                        Files.createDirectories(file.getParent());
                        Files.write(file, data, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        return data;
                    });
        }).thenCompose(Function.identity());

        if (cache) {
            synchronized (this.replicationBlobs) {
                CompletableFuture<byte[]> replacedFuture = this.replicationBlobs.putAndMoveToFirst(path, future);
                checkState(replacedFuture == null, "replaced existing cached future for '%s'", path);
                this.cleanupReplicationDataObjectCache();
            }
        }
        return future;
    }

    private CompletableFuture<byte[]> downloadUrl(@NonNull String url) throws Exception {
        return CompletableFuture.supplyAsync((ESupplier<byte[]>) () -> {
            long retryDelay = 0L;
            do {
                logger.trace("downloading URL '%s'...", url);
                try {
                    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
                    connection.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(5L));
                    connection.setReadTimeout((int) TimeUnit.SECONDS.toMillis(5L));
                    connection.connect();

                    switch (connection.getResponseCode()) {
                        case 200:
                            try (InputStream in = connection.getInputStream()) {
                                byte[] data = StreamUtil.toByteArray(in);
                                logger.info("downloaded data for URL '%s' (%d bytes)", url, data.length);
                                return data;
                            }
                        default:
                            logger.alert("URL '%s' responded with unexpected status code '%d %s'", url, connection.getResponseCode(), connection.getResponseMessage());
                            throw new IOException(connection.getResponseCode() + " " + connection.getResponseMessage());
                    }
                } catch (SocketTimeoutException e) {
                    //no-op, fall through to timeout handler code
                } catch (IOException e) {
                    if (!e.getMessage().toLowerCase(Locale.ROOT).contains("timed out")) {
                        logger.alert("failed to download URL '%s'", e, url);
                        throw PUnsafe.throwException(e);
                    }
                } catch (Throwable t) {
                    logger.alert("failed to download URL '%s'", t, url);
                    throw PUnsafe.throwException(t);
                }

                retryDelay = Math.min(retryDelay + 10L, 60L);
                logger.warn("connection timed out while downloading URL '%s', trying again in %d seconds...", url, retryDelay);
                PorkUtil.sleep(TimeUnit.SECONDS.toMillis(retryDelay));
            } while (true);
        });
    }

    public synchronized Handle<Path> getTmpFilePath(@NonNull String prefix, @NonNull String extension) throws IOException {
        Path path;
        do {
            path = this.tmpDirectoryPath.resolve(prefix + '-' + UUID.randomUUID() + '.' + extension);
        } while (this.activeTmpFiles.contains(path) || PFiles.checkFileExists(path));

        PFiles.ensureDirectoryExists(this.tmpDirectoryPath);

        @AllArgsConstructor
        class PathHandle extends AbstractRefCounted implements Handle<Path> {
            Path path;

            @Override
            public PathHandle retain() throws AlreadyReleasedException {
                super.retain();
                return this;
            }

            @Override
            protected void doRelease() {
                synchronized (Storage.this) {
                    if (!Storage.this.activeTmpFiles.remove(this.path)) {
                        logger.alert("temporary file '%s' was already released?!?", this.path);
                    }
                    if (PFiles.checkFileExists(this.path)) {
                        logger.alert("temporary file '%s' still exists?!?", this.path);
                        PFiles.rm(this.path);
                    }
                    this.path = null;
                }
            }

            @Override
            public Path get() {
                this.ensureNotReleased();
                return this.path;
            }
        }

        this.activeTmpFiles.add(path);
        return new PathHandle(path);
    }
}
