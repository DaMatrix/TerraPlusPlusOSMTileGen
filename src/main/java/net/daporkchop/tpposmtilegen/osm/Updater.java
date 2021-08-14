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

package net.daporkchop.tpposmtilegen.osm;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.osm.changeset.Changeset;
import net.daporkchop.tpposmtilegen.osm.changeset.ChangesetState;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.Arrays;
import java.util.function.LongPredicate;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * Updates the OSM dataset.
 *
 * @author DaPorkchop_
 */
public class Updater {
    protected final ChangesetState globalState;

    public Updater(@NonNull Storage storage) throws Exception {
        this.globalState = storage.getChangesetState();

        long sequenceNumber = storage.sequenceNumber().get(storage.db().read());
        if (sequenceNumber < 0L) { //compute sequence number
            logger.info("attempting to compute sequence number from timestamp...");

            long replicationTimestamp = storage.replicationTimestamp().get();
            checkState(replicationTimestamp >= 0L, "no replication info!");

            //binary search to find sequence number from timestamp
            long min = 0;
            long max = this.globalState.sequenceNumber();
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
        } else if (storage.replicationTimestamp().get() < 0L) { //set timestamp
            storage.replicationTimestamp().set(storage.getChangesetState(sequenceNumber).timestamp().toEpochMilli() / 1000L);
        }
    }

    public boolean update(@NonNull Storage storage, @NonNull DBAccess access) throws Exception {
        long sequenceNumber = storage.sequenceNumber().get(access);

        Instant now = Instant.ofEpochSecond(storage.replicationTimestamp().get());
        logger.info("current: %d (%s)\nlatest: %d (%s)", sequenceNumber, now, this.globalState.sequenceNumber(), this.globalState.timestamp());

        if (sequenceNumber >= this.globalState.sequenceNumber()) {
            return false;
        }

        logger.info("updating...");
        ChangesetState state = storage.getChangesetState(sequenceNumber);
        long next = sequenceNumber + 1L;
        ChangesetState nextState = storage.getChangesetState(next);
        logger.trace("updating from %d (%s) to %d (%s)\n", sequenceNumber, state.timestamp(), next, nextState.timestamp());

        Changeset changeset = storage.getChangeset(next);
        this.applyChanges(storage, access, now, changeset);
        storage.sequenceNumber().set(access, next);
        return true;
    }

    private void applyChanges(Storage storage, DBAccess access, Instant now, Changeset changeset) throws Exception {
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

        //pass 3: convert geometry of all changed elements to GeoJSON and recompute relations
        for (long combinedId : changedIds) {
            storage.convertToGeoJSONAndStoreInDB(access, combinedId, null, true);
        }
        logger.trace("pass 3: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));

        //pass 4: write updated tiles
        storage.exportDirtyTiles(access);
        logger.trace("pass 4: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));
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
            Point point = storage.points().get(access, id);
            checkState(point != null, "node with id %d doesn't exist", id);

            storage.points().delete(access, id);
            storage.nodes().delete(access, id);
        } else if (element instanceof Changeset.Way) {
            Way way = storage.ways().get(access, id);
            checkState(way != null, "way with id %d doesn't exist", id);

            storage.ways().delete(access, id);
        } else if (element instanceof Changeset.Relation) {
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
