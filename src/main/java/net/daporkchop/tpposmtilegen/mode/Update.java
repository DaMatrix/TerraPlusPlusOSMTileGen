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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Scanner;
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
                        Matcher matcher = Pattern.compile("^/tile/(\\d+)/(\\d+)\\.json$").matcher(exchange.getRequestURI().getPath());
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
                server.stop(5000);
                return;
            }

            ChangesetState globalState = storage.getChangesetState();

            if (storage.sequenceNumber().get() < 0L) { //compute sequence number
                logger.info("attempting to compute sequence number from timestamp...");

                long replicationTimestamp = storage.replicationTimestamp().get();
                checkState(replicationTimestamp >= 0L, "no replication info!");

                //binary search to find sequence number from timestamp
                int min = 0;
                int max = globalState.sequenceNumber();
                while (min <= max) {
                    int middle = (min + max) >> 1;
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
                        min = middle + 1;
                    } else {
                        logger.trace("sequence number too low: " + middle);
                        max = middle - 1;
                    }
                }

                logger.success("offsetting sequence number by 60 to be sure we have the right one", min - 60);
                storage.sequenceNumber().set(min - 60);
            } else if (storage.replicationTimestamp().get() < 0L) { //set timestamp
                storage.replicationTimestamp().set(storage.getChangesetState(toInt(storage.sequenceNumber().get())).timestamp().toEpochMilli() / 1000L);
            }

            int sequenceNumber = toInt(storage.sequenceNumber().get());
            Instant now = Instant.ofEpochSecond(storage.replicationTimestamp().get());
            logger.info("current: %d (%s)\nlatest: %d (%s)", sequenceNumber, now, globalState.sequenceNumber(), globalState.timestamp());

            if (sequenceNumber >= globalState.sequenceNumber()) {
                logger.info("nothing to do...");
                return;
            }

            try (DBAccess access = storage.db().newNotAutoFlushingWriteBatch()) { //clear anything that might be left over in the temp geojson storage
                storage.tempJsonStorage().clear(access);
            }

            logger.info("updating...");
            ChangesetState state = storage.getChangesetState(sequenceNumber);
            for (; sequenceNumber < globalState.sequenceNumber(); sequenceNumber++) {
                int next = sequenceNumber + 1;
                ChangesetState nextState = storage.getChangesetState(next);
                logger.trace("updating from %d (%s) to %d (%s)\n", sequenceNumber, state.timestamp(), next, nextState.timestamp());

                Changeset changeset = storage.getChangeset(next);
                try (DBAccess access = storage.db().newTransaction()) {
                    this.applyChanges(storage, access, dst, now, changeset);
                }

                state = nextState;
            }
        }
    }

    private void applyChanges(Storage storage, DBAccess access, Path tileDir, Instant now, Changeset changeset) throws Exception {
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

        logger.debug("batched %.2fMiB of updates\n", access.getDataSize() / (1024.0d * 1024.0d));

        LongSet changedIdsProcessed = new LongOpenHashSet();
        LongSet dirtyTiles = new LongOpenHashSet();
        while (!changedIds.isEmpty()) {
            LongIterator itr = changedIds.iterator();
            long id = itr.nextLong();
            itr.remove();

            //find and recursively process all elements that reference this one
            LongList referents = new LongArrayList();
            storage.references().getReferencesTo(access, 0, id, referents);
            if (!referents.isEmpty()) {
                storage.references().deleteReferencesTo(access, 0, id);
                referents.removeAll(changedIdsProcessed);
                changedIds.addAll(referents);
            }

            //mark all tiles that were intersected as dirty
            long[] intersectedTiles = storage.intersectedTiles().get(access, id);
            if (intersectedTiles != null) {
                storage.intersectedTiles().deleteAll(access, LongLists.singleton(id));
                dirtyTiles.addAll(LongArrayList.wrap(intersectedTiles));
            }

            changedIdsProcessed.add(id);
        }

        for (LongIterator itr = changedIdsProcessed.iterator(); itr.hasNext();) {
            long id = itr.nextLong();
            Element element = storage.getElement(access, id);
            if (element != null) { //if it's null the element was deleted, which we don't care about handling
                element.computeReferences(access, storage);
            } else {
                itr.remove();
            }
        }

        LongSet elementsToRegenerate = new LongOpenHashSet(changedIdsProcessed);
        for (long tilePos : dirtyTiles) {
            LongList elementsInTile = new LongArrayList();
            storage.tileContents().getElementsInTile(access, tilePos, elementsInTile);
            if (!elementsInTile.isEmpty()) {
                storage.tileContents().clearTile(access, tilePos);
                elementsToRegenerate.addAll(elementsInTile);
            }
        }

        for (long element : elementsToRegenerate) {
            storage.convertToGeoJSONAndStoreInDB(access, tileDir, element);
        }

        storage.exportDirtyTiles(access, tileDir);

        storage.tempJsonStorage().clear(access);

        logger.debug("batched %.2fMiB of updates\n", access.getDataSize() / (1024.0d * 1024.0d));
    }

    private void create(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        long id = element.id();
        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;

            storage.points().put(access, id, new Point(changedNode.lon(), changedNode.lat()));
            if (!changedNode.tags().isEmpty()) {
                storage.nodes().put(access, id, new Node(id, changedNode.tags()));
            }

            changedIds.add(Element.addTypeToId(Node.TYPE, id));
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;

            storage.ways().put(access, id, new Way(id, changedWay.tags(), changedWay.refs().toLongArray()));

            changedIds.add(Element.addTypeToId(Way.TYPE, id));
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;

            Relation.Member[] members = changedRelation.members().stream().map(Relation.Member::new).toArray(Relation.Member[]::new);
            storage.relations().put(access, id, new Relation(id, changedRelation.tags(), members));

            changedIds.add(Element.addTypeToId(Relation.TYPE, id));
        }
    }

    private void modify(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        long id = element.id();
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
                if (storage.nodes().get(access, id) != null) {
                    storage.nodes().deleteAll(access, LongLists.singleton(id));
                }
            } else {
                storage.nodes().put(access, id, new Node(id, changedNode.tags()));
            }

            changedIds.add(Element.addTypeToId(Node.TYPE, id));
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
                    storage.references().addReference(access, Node.TYPE, node, Way.TYPE, id);
                }
            }
            for (long node : oldNodes) { //all nodes that remain are no longer referenced
                storage.references().deleteReference(access, Node.TYPE, node, Way.TYPE, id);
            }

            storage.ways().put(access, id, new Way(id, changedWay.tags(), changedWay.refs().toLongArray()));

            changedIds.add(Element.addTypeToId(Way.TYPE, id));
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
                    storage.references().addReference(access, 0, ref, Relation.TYPE, id);
                }
            }
            for (long ref : oldRefs) { //all elements that remain are no longer referenced
                storage.references().deleteReference(access, 0, ref, Relation.TYPE, id);
            }

            storage.relations().put(access, id, new Relation(id, changedRelation.tags(), newMembers));

            changedIds.add(Element.addTypeToId(Relation.TYPE, id));
        }
    }

    private void delete(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        long id = element.id();

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;
            Point point = storage.points().get(access, id);
            checkState(point != null, "node with id %d doesn't exist", id);

            storage.points().deleteAll(access, LongLists.singleton(id));
            storage.nodes().deleteAll(access, LongLists.singleton(id));
            changedIds.add(Element.addTypeToId(Node.TYPE, id));
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;
            Way way = storage.ways().get(access, id);
            checkState(way != null, "way with id %d doesn't exist", id);

            storage.ways().deleteAll(access, LongLists.singleton(id));
            changedIds.add(Element.addTypeToId(Way.TYPE, id));
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;
            Relation relation = storage.relations().get(access, id);
            checkState(relation != null, "relation with id %d doesn't exist", id);

            storage.relations().deleteAll(access, LongLists.singleton(id));
            changedIds.add(Element.addTypeToId(Relation.TYPE, id));
        }
    }
}
