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
import org.rocksdb.Checkpoint;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.UInt64AddOperator;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Locale;
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

    protected final LongArrayDB[] intersectedTiles = new LongArrayDB[MAX_LEVELS];
    protected final TileDB[] tileJsonStorage = new TileDB[MAX_LEVELS];
    protected final BlobDB[] externalJsonStorage = new BlobDB[MAX_LEVELS];

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

        IntStream.range(0, MAX_LEVELS).forEach(lvl -> builder.add("intersected_tiles@" + lvl, (database, handle, descriptor) -> this.intersectedTiles[lvl] = new LongArrayDB(database, handle, descriptor)));
        IntStream.range(0, MAX_LEVELS).forEach(lvl -> builder.add("tiles@" + lvl, DatabaseConfig.ColumnFamilyType.COMPACT, (database, handle, descriptor) -> this.tileJsonStorage[lvl] = legacy ? new TileDB.Legacy(database, handle, descriptor) : new TileDB(database, handle, descriptor), legacy ? null : UInt64ToBlobMapMergeOperator.INSTANCE));
        IntStream.range(0, MAX_LEVELS).forEach(lvl -> builder.add("external_json@" + lvl, DatabaseConfig.ColumnFamilyType.COMPACT, (database, handle, descriptor) -> this.externalJsonStorage[lvl] = new BlobDB(database, handle, descriptor)));
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

    public void convertToGeoJSONAndStoreInDB(@NonNull DBAccess access, long combinedId, Element element, boolean allowUnknown, boolean assumeEmpty) throws Exception {
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

        //pass 1: delete element from everything (we skip this step if the database is initially empty)
        if (!assumeEmpty) {
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
