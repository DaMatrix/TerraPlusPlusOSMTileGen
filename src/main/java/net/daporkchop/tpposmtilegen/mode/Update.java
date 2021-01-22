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

package net.daporkchop.tpposmtilegen.mode;

import com.sun.net.httpserver.HttpServer;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.osm.changeset.Changeset;
import net.daporkchop.tpposmtilegen.osm.changeset.ChangesetState;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
public class Update implements IMode {
    @Override
    public String name() {
        return "update";
    }

    @Override
    public String synopsis() {
        return "<index_dir> <tile_dir>";
    }

    @Override
    public String help() {
        return "Updates the index and tiles by applying the latest changesets from the OpenStreetMap database.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: update <index_dir> <tile_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        Path dst = Paths.get(args[1]);

        try (Storage storage = new Storage(src.toPath())) {
            if (false) {
                HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
                server.createContext("/tile/", exchange -> {
                    try {
                        Matcher matcher = Pattern.compile("^/tile/(-?\\d+)/(-?\\d+)\\.json$").matcher(exchange.getRequestURI().getPath());
                        checkArg(matcher.find());
                        int tileX = Integer.parseInt(matcher.group(1));
                        int tileY = Integer.parseInt(matcher.group(2));
                        long tilePos = xy2tilePos(tileX, tileY);

                        LongList elements = new LongArrayList();
                        storage.tileContents().getElementsInTile(storage.db().read(), tilePos, elements);

                        ByteBuffer[] buffers = storage.tempJsonStorage().getAll(storage.db().read(), elements).toArray(new ByteBuffer[0]);
                        exchange.sendResponseHeaders(200, 0);

                        try (OutputStream out = exchange.getResponseBody()) {
                            for (ByteBuffer buffer : buffers) {
                                out.write(buffer.array(), buffer.arrayOffset(), buffer.remaining());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(exchange.getRequestURI().getPath(), e);
                    }
                });
                server.createContext("/", exchange -> {
                    Path path = dst.resolve(exchange.getRequestURI().getPath().substring(1));
                    if (Files.isRegularFile(path)) {
                        exchange.sendResponseHeaders(200, 0);
                        try (InputStream in = new FileInputStream(path.toFile());
                             OutputStream out = exchange.getResponseBody()) {
                            byte[] buf = new byte[4096];
                            for (int i; (i = in.read(buf)) > 0; ) {
                                out.write(buf, 0, i);
                            }
                        }
                    } else {
                        exchange.sendResponseHeaders(404, 0);
                        exchange.getResponseBody().close();
                    }
                });
                server.start();

                new Scanner(System.in).nextLine();
                server.stop(0);
                return;
            }

            ChangesetState globalState = storage.getChangesetState();

            long sequenceNumber = storage.sequenceNumber().get(storage.db().read());
            if (sequenceNumber < 0L) { //compute sequence number
                logger.info("attempting to compute sequence number from timestamp...");

                long replicationTimestamp = storage.replicationTimestamp().get();
                checkState(replicationTimestamp >= 0L, "no replication info!");

                //binary search to find sequence number from timestamp
                long min = 0;
                long max = globalState.sequenceNumber();
                while (min <= max) {
                    long middle = (min + max) >> 1L;
                    long middleTimestamp;
                    try {
                        middleTimestamp = storage.getChangesetState(middle).timestamp().toEpochMilli() / 1000L;
                    } catch (FileNotFoundException e) {
                        middleTimestamp = -1L; //fallback if file not found, assume we're too low
                    }

                    if (middleTimestamp == replicationTimestamp) {
                        logger.success("found sequence number: " + middle);
                    } else if (middleTimestamp < replicationTimestamp) {
                        logger.trace("sequence number too high: " + middle);
                        min = middle + 1L;
                    } else {
                        logger.trace("sequence number too low: " + middle);
                        max = middle - 1L;
                    }
                }

                logger.success("offsetting sequence number %d by 60 to be sure we have the right one -> %d", min, min - 60L);
                try (DBAccess batch = storage.db().newNotAutoFlushingWriteBatch()) {
                    storage.sequenceNumber().set(batch, min - 60L);
                }
                sequenceNumber = min - 60L;
            } else if (storage.replicationTimestamp().get() < 0L) { //set timestamp
                storage.replicationTimestamp().set(storage.getChangesetState(sequenceNumber).timestamp().toEpochMilli() / 1000L);
            }

            Instant now = Instant.ofEpochSecond(storage.replicationTimestamp().get());
            logger.info("current: %d (%s)\nlatest: %d (%s)", sequenceNumber, now, globalState.sequenceNumber(), globalState.timestamp());

            if (sequenceNumber >= globalState.sequenceNumber()) {
                logger.info("nothing to do...");
                return;
            }

            logger.info("updating...");
            ChangesetState state = storage.getChangesetState(sequenceNumber);
            try (DBAccess txn = storage.db().newTransaction()) {
                for (; sequenceNumber < globalState.sequenceNumber(); sequenceNumber = storage.sequenceNumber().get(txn)) {
                    long next = sequenceNumber + 1L;
                    ChangesetState nextState = storage.getChangesetState(next);
                    logger.trace("updating from %d (%s) to %d (%s)\n", sequenceNumber, state.timestamp(), next, nextState.timestamp());

                    Changeset changeset = storage.getChangeset(next);
                    this.applyChanges(storage, txn, dst, now, changeset);
                    storage.sequenceNumber().set(txn, next);

                    state = nextState;
                }
            } catch (Throwable t) {
                logger.alert(t);
            }
        }
    }

    private void applyChanges(Storage storage, DBAccess access, Path tileDir, Instant now, Changeset changeset) throws Exception {
        //pass 1: find all elements affected by this change, and write out modified elements
        LongSet changedIds = new LongOpenHashSet();
        for (Changeset.Entry entry : changeset.entries()) {
            entry.elements().removeIf(element -> !element.timestamp().isAfter(now));
            switch (entry.op()) {
                case CREATE:
                    for (Changeset.Element element : entry.elements()) {
                        this.create(storage, access, element, changedIds);
                    }
                    break;
                case MODIFY:
                    for (Changeset.Element element : entry.elements()) {
                        this.modify(storage, access, element, changedIds);
                    }
                    break;
                case DELETE:
                    for (Changeset.Element element : entry.elements()) {
                        this.delete(storage, access, element, changedIds);
                    }
                    break;
            }
        }
        logger.trace("pass 1: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));

        //pass 2: update element references
        for (long combinedId : changedIds) {
            storage.references().deleteReferencesTo(access, combinedId);

            Element element = storage.getElement(access, combinedId);
            if (element != null) {
                element.computeReferences(access, storage);
            }
        }
        logger.trace("pass 2: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));

        //pass 3: update geometry intersections
        LongSet dirtyTiles = new LongOpenHashSet();
        Long2IntMap intersectedTileCounts = new Long2IntOpenHashMap();
        for (long combinedId : changedIds) {
            LongSet intersectedTilesBefore;
            {
                long[] arr = storage.intersectedTiles().get(access, combinedId);
                intersectedTilesBefore = new LongOpenHashSet(arr != null ? arr : new long[0]);
            }

            Element element = storage.getElement(access, combinedId);
            LongSet intersectedTilesNext = LongSets.EMPTY_SET;
            if (element != null) {
                Geometry geometry = element.toGeometry(storage, access);
                if (geometry != null) {
                    long[] intersectedTilesNextArray = geometry.listIntersectedTiles();
                    storage.intersectedTiles().put(access, combinedId, intersectedTilesNextArray);

                    intersectedTilesNext = new LongOpenHashSet(intersectedTilesNextArray);
                } else {
                    storage.intersectedTiles().delete(access, combinedId);
                }
            } else { //element was deleted
                storage.intersectedTiles().delete(access, combinedId);
            }

            LongList toDeleteTiles = new LongArrayList(intersectedTilesBefore);
            toDeleteTiles.removeAll(intersectedTilesNext);
            storage.tileContents().deleteElementFromTiles(access, toDeleteTiles, combinedId);

            LongList toAddTiles = new LongArrayList(intersectedTilesNext);
            toAddTiles.removeAll(intersectedTilesBefore);
            storage.tileContents().addElementToTiles(access, toAddTiles, combinedId);

            dirtyTiles.addAll(intersectedTilesBefore);
            dirtyTiles.addAll(intersectedTilesNext);
            intersectedTileCounts.put(combinedId, intersectedTilesNext.size());
        }
        logger.trace("pass 3: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));

        //pass 4: find all elements intersecting the changed tiles
        LongSet toRegenerateElements = new LongOpenHashSet();
        for (long tilePos : dirtyTiles) {
            LongList elements = new LongArrayList();
            storage.tileContents().getElementsInTile(access, tilePos, elements);
            toRegenerateElements.addAll(elements);
        }
        logger.trace("pass 4: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));

        //pass 5: convert geometry of all elements intersecting all changed tiles to GeoJSON
        List<Path> toDeleteFiles = new ArrayList<>();
        Long2ObjectMap<ByteBuffer> jsons = new Long2ObjectOpenHashMap<>(toRegenerateElements.size());
        Set<ByteBuffer> allBuffers = Collections.newSetFromMap(new IdentityHashMap<>());
        for (long combinedId : toRegenerateElements) {
            Element element = storage.getElement(access, combinedId);
            if (element != null) {
                Geometry geometry = element.toGeometry(storage, access);
                if (geometry != null) {
                    //encode geometry to GeoJSON
                    StringBuilder builder = new StringBuilder();
                    Geometry.toGeoJSON(builder, geometry, element.tags(), combinedId);

                    //convert json to bytes
                    ByteBuffer json = Geometry.toBytes(builder);

                    //figure out how many tiles this element is in
                    int tileCount = intersectedTileCounts.getOrDefault(combinedId, -1);
                    if (tileCount < 0) {
                        tileCount = storage.intersectedTiles().get(access, combinedId).length;
                    }

                    if (!geometry.shouldStoreExternally(tileCount, builder.length())) {
                        //the element's geometry is small enough that storing it in multiple tiles should be a non-issue
                        checkState(allBuffers.add(json));
                        jsons.put(combinedId, json);

                        if (changedIds.contains(combinedId)) {
                            Path externalFile = storage.externalFile(tileDir, geometry, combinedId);
                            if (Files.exists(externalFile)) {
                                //enqueue unused json file for deletion
                                toDeleteFiles.add(externalFile);
                            }
                        }
                    } else {
                        ByteBuffer reference;
                        if (changedIds.contains(combinedId)) {
                            //write external storage file and store reference in map instead
                            reference = storage.writeExternal(tileDir, geometry, combinedId, json);
                        } else {
                            reference = storage.createReference(geometry, combinedId);
                        }

                        checkState(allBuffers.add(reference));
                        jsons.put(combinedId, reference);
                        PUnsafe.pork_releaseBuffer(json);
                    }
                }
            }
        }
        logger.trace("pass 5: batched %.2fMiB of updates, %d files queued for deletion, %d tiles queued for regeneration", access.getDataSize() / (1024.0d * 1024.0d), toDeleteFiles.size(), dirtyTiles.size());
        
        //pass 6: write updated tiles
        for (long tilePos : dirtyTiles) {
            LongSet elements = new LongOpenHashSet(); //using a set instead of a list because the transaction iterator can cause duplicate elements to appear
            storage.tileContents().getElementsInTile(access, tilePos, elements);
            int size = elements.size();

            Path tileFile = storage.tileFile(tileDir, tilePos);
            if (size != 0) {
                ByteBuffer[] buffers = new ByteBuffer[size];
                long totalSize = 0L;
                int i = 0;
                for (long id : elements) {
                    ByteBuffer buffer = jsons.get(id);
                    checkState(buffer != null, "unable to find json data for %s %d!",
                            Element.typeName(Element.extractType(id)), Element.extractId(id));
                    checkState(buffer.hasRemaining(), "json data for %s %d has no data remaining!",
                            Element.typeName(Element.extractType(id)), Element.extractId(id));
                    totalSize += buffer.remaining();
                    buffers[i++] = buffer;
                }

                Files.createDirectories(tileFile.getParent());
                try (FileChannel channel = FileChannel.open(tileFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    long written = 0L;
                    do {
                        written += channel.write(buffers);
                    } while (written != totalSize);
                }

                for (ByteBuffer buffer : buffers) {
                    checkState(!buffer.hasRemaining(), "didn't write all data from buffer!");
                    buffer.rewind(); //reset index to 0
                }
            } else { //the tile no longer has any contents, delete it
                toDeleteFiles.add(tileFile);
            }
        }
        logger.trace("pass 6: batched %.2fMiB of updates, %d files queued for deletion", access.getDataSize() / (1024.0d * 1024.0d), toDeleteFiles.size());

        //pass 7: delete queued files and release buffers
        jsons.values().forEach(PUnsafe::pork_releaseBuffer);
        for (int i = toDeleteFiles.size() - 1; i >= 0; i--) { //iterate backwards to ensure that tiles are deleted before external files
            logger.debug("Deleting %s", toDeleteFiles.get(i));
            Files.deleteIfExists(toDeleteFiles.get(i));
        }
    }

    private void create(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        long id = element.id();
        long combinedId = Element.addTypeToId(element.type(), id);

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;

            storage.points().put(access, id, new Point(changedNode.lon(), changedNode.lat()));
            if (!changedNode.tags().isEmpty()) {
                storage.nodes().put(access, id, new Node(id, changedNode.tags()));
            }
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;

            storage.ways().put(access, id, new Way(id, changedWay.tags(), changedWay.refs().toLongArray()));
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;

            Relation.Member[] members = changedRelation.members().stream().map(Relation.Member::new).toArray(Relation.Member[]::new);
            storage.relations().put(access, id, new Relation(id, changedRelation.tags(), members));
        }

        //this will force the element's tile intersections and references to be re-computed
        changedIds.add(combinedId);
    }

    private void modify(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        long id = element.id();
        long combinedId = Element.addTypeToId(element.type(), id);

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;
            Point point = storage.points().get(access, id);
            if (point == null) {
                logger.warn("attempting to modify non-existent node with id %d, assuming that it's newly re-created!", id);
                this.create(storage, access, element, changedIds);
                return;
            }

            storage.points().put(access, id, new Point(changedNode.lon(), changedNode.lat()));
            if (changedNode.tags().isEmpty()) {
                storage.nodes().delete(access, id);
            } else {
                storage.nodes().put(access, id, new Node(id, changedNode.tags()));
            }
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;
            Way way = storage.ways().get(access, id);
            if (way == null) {
                logger.warn("attempting to modify non-existent way with id %d, assuming that it's newly re-created!", id);
                this.create(storage, access, element, changedIds);
                return;
            }

            LongSet oldNodes = new LongOpenHashSet(way.nodes());
            LongSet newNodes = new LongOpenHashSet(changedWay.refs());
            for (long node : newNodes) {
                if (!oldNodes.remove(node)) { //node was newly added to this way
                    storage.references().addReference(access, Element.addTypeToId(Node.TYPE, node), combinedId);
                }
            }
            for (long node : oldNodes) { //all nodes that remain are no longer referenced
                storage.references().deleteReference(access, Element.addTypeToId(Node.TYPE, node), combinedId);
            }

            storage.ways().put(access, id, new Way(id, changedWay.tags(), changedWay.refs().toLongArray()));
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;
            Relation relation = storage.relations().get(access, id);
            if (relation == null) {
                logger.warn("attempting to modify non-existent relation with id %d, assuming that it's newly re-created!", id);
                this.create(storage, access, element, changedIds);
                return;
            }

            Relation.Member[] newMembers = changedRelation.members().stream().map(Relation.Member::new).toArray(Relation.Member[]::new);

            LongSet oldRefs = new LongOpenHashSet(Arrays.stream(relation.members()).mapToLong(Relation.Member::combinedId).toArray());
            LongSet newRefs = new LongOpenHashSet(Arrays.stream(newMembers).mapToLong(Relation.Member::combinedId).toArray());
            for (long ref : newRefs) {
                if (!oldRefs.remove(ref)) { //element was newly added to this relation
                    storage.references().addReference(access, ref, combinedId);
                }
            }
            for (long ref : oldRefs) { //all elements that remain are no longer referenced
                storage.references().deleteReference(access, ref, combinedId);
            }

            storage.relations().put(access, id, new Relation(id, changedRelation.tags(), newMembers));
        }

        //this will force the element's tile intersections and references to be re-computed
        changedIds.add(combinedId);
        this.markReferentsDirty(storage, access, changedIds, combinedId);
    }

    private void delete(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        long id = element.id();
        long combinedId = Element.addTypeToId(element.type(), id);

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;
            Point point = storage.points().get(access, id);
            checkState(point != null, "node with id %d doesn't exist", id);

            storage.points().delete(access, id);
            storage.nodes().delete(access, id);
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;
            Way way = storage.ways().get(access, id);
            checkState(way != null, "way with id %d doesn't exist", id);

            storage.ways().delete(access, id);
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;
            Relation relation = storage.relations().get(access, id);
            checkState(relation != null, "relation with id %d doesn't exist", id);

            storage.relations().delete(access, id);
        }

        //this will force the element's tile intersections and references to be re-computed
        changedIds.add(combinedId);
        this.markReferentsDirty(storage, access, changedIds, combinedId);
    }

    private void markReferentsDirty(Storage storage, DBAccess access, LongSet changedIds, long combinedId) throws Exception {
        LongList referents = new LongArrayList();
        storage.references().getReferencesTo(access, combinedId, referents);

        referents.removeIf((LongPredicate) l -> !changedIds.add(l));

        for (int i = 0, size = referents.size(); i < size; i++) {
            long referent = referents.getLong(i);
            if (changedIds.add(referent)) {
                this.markReferentsDirty(storage, access, changedIds, referent);
            }
        }
    }
}
