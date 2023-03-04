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

package net.daporkchop.tpposmtilegen.osm;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.osm.changeset.Changeset;
import net.daporkchop.tpposmtilegen.osm.changeset.ChangesetState;
import net.daporkchop.tpposmtilegen.osm.changeset.Operation;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * Updates the OSM dataset.
 *
 * @author DaPorkchop_
 */
@Getter
public class Updater {
    protected final ChangesetState globalState;

    public Updater(@NonNull Storage storage) throws Exception {
        this.globalState = storage.getLatestChangesetState().join();

        OptionalLong sequenceNumber = storage.sequenceNumberProperty().getLong(storage.db().read());
        if (!sequenceNumber.isPresent()) { //compute sequence number
            logger.info("attempting to compute sequence number from timestamp %s...",
                    Instant.ofEpochSecond(storage.replicationTimestampProperty().getLong(storage.db().read()).getAsLong()));

            long replicationTimestamp = storage.replicationTimestampProperty().getLong(storage.db().read()).getAsLong();
            checkState(replicationTimestamp >= 0L, "no replication info!");

            //binary search to find sequence number from timestamp
            long min = 0L;
            long max = this.globalState.sequenceNumber();
            while (min <= max) {
                long middle = (min + max) >> 1L;
                long middleTimestamp;
                try {
                    middleTimestamp = storage.getChangesetState(middle, null).join().timestamp().toEpochMilli() / 1000L;
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
            try (DBWriteAccess batch = storage.db().newNotAutoFlushingWriteBatch()) {
                storage.sequenceNumber().set(batch, min - 60L);
            }
        } else if (!storage.replicationTimestampProperty().getLong(storage.db().read()).isPresent()) { //set timestamp
            try (DBWriteAccess batch = storage.db().beginLocalBatch()) {
                storage.replicationTimestampProperty().set(batch, storage.getChangesetState(sequenceNumber.getAsLong(), null).join().timestamp().toEpochMilli() / 1000L);
            }
        }
    }

    public boolean update(@NonNull Storage storage, @NonNull DBAccess access) throws Exception {
        long sequenceNumber = storage.sequenceNumberProperty().getLong(access).getAsLong();

        Instant now = Instant.ofEpochSecond(storage.replicationTimestampProperty().getLong(access).getAsLong());
        logger.info("current: %d (%s)\nlatest: %d (%s)", sequenceNumber, now, this.globalState.sequenceNumber(), this.globalState.timestamp());

        if (sequenceNumber >= this.globalState.sequenceNumber()) {
            return false;
        }

        logger.info("updating...");
        long next = sequenceNumber + 1L;

        CompletableFuture<ChangesetState> stateFuture = storage.getChangesetState(sequenceNumber, this.globalState);
        CompletableFuture<ChangesetState> nextStateFuture = storage.getChangesetState(next, this.globalState);
        CompletableFuture<Changeset> changesetFuture = storage.getChangeset(next, this.globalState);

        ChangesetState state = stateFuture.join();
        ChangesetState nextState = nextStateFuture.join();
        logger.trace("updating from %d (%s) to %d (%s)\n", sequenceNumber, state.timestamp(), next, nextState.timestamp());

        if (access instanceof DBWriteAccess.Transactional) {
            ((DBWriteAccess.Transactional) access).pushCheckpoint();
        }

        Changeset changeset = changesetFuture.join();
        try {
            this.applyChanges(storage, access, now, changeset);
            storage.sequenceNumberProperty().set(access, next);
            return true;
        } catch (Exception e) {
            if (access instanceof DBWriteAccess.Transactional) { //roll back uncommitted changes in the transaction which resulted in the error
                ((DBWriteAccess.Transactional) access).popCheckpoint();
            }
            throw PUnsafe.throwException(e);
        }
    }

    private void applyChanges(Storage storage, DBAccess access, Instant now, Changeset changeset) throws Exception {
        @Data
        class TaggedElement {
            @NonNull
            final Operation op;
            @NonNull
            final Changeset.Element element;
        }

        //pass 1: find all elements affected by this change, and write out modified elements
        List<TaggedElement> taggedElements = changeset.entries().stream()
                .flatMap(entry -> entry.elements().stream().map(element -> new TaggedElement(entry.op(), element)))
                .filter(taggedElement -> taggedElement.element().timestamp().isAfter(now))
                .sorted(Comparator.comparingInt(taggedElement -> taggedElement.element().version()))
                .collect(Collectors.toList());
        logger.info("processing %d/%d changes in %d entries", taggedElements.size(), changeset.entries().stream().mapToInt(entry -> entry.elements().size()).sum(), changeset.entries().size());

        LongSet changedIds = new LongOpenHashSet();
        for (TaggedElement taggedElement : taggedElements) {
            Changeset.Element element = taggedElement.element();
            switch (taggedElement.op()) {
                case CREATE:
                    this.create(storage, access, element, changedIds);
                    break;
                case MODIFY:
                    this.modify(storage, access, element, changedIds);
                    break;
                case DELETE:
                    this.delete(storage, access, element, changedIds);
                    break;
                default:
                    throw new UnsupportedOperationException(taggedElement.op().name());
            }
        }
        logger.trace("pass 1: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));

        //pass 2: update element references
        if (false) {
            for (long combinedId : changedIds) {
                storage.references().deleteAllReferencesTo(access, combinedId);

                Element element = storage.getElement(access, combinedId);
                if (element != null) {
                    element.computeReferences(access, storage);
                }
            }
            logger.trace("pass 2: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));
        }

        //pass 3: convert geometry of all changed elements to GeoJSON and recompute relations
        for (long combinedId : changedIds) {
            storage.convertToGeoJSONAndStoreInDB(access, combinedId, null, true);
        }
        logger.trace("pass 3: batched %.2fMiB of updates", access.getDataSize() / (1024.0d * 1024.0d));
    }

    private void create(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        int type = element.type();
        long id = element.id();
        long combinedId = Element.addTypeToId(type, id);

        if (element.version() != 1) {
            logger.alert("attempting to create %s with id %d at non-one version %d! element: %s", Element.typeName(type), id, element.version(), element);
            throw new UpdateImpossibleException();
        } else if (!storage.references().getReferencesTo(access, combinedId).isEmpty()) {
            logger.alert("attempting to create %s with id %d, but it's already referenced?!? element: %s\nreferences: %s", Element.typeName(type), id, element.version(), element, storage.references().getReferencesTo(access, combinedId));
            throw new UpdateImpossibleException();
        }

        //we assume that an element which has just been created isn't immediately deleted, lol
        boolean visible = true;

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;

            Node newNode = new Node(id, changedNode.tags(), changedNode.version(), visible);
            storage.nodes().put(access, id, newNode);
            storage.points().put(access, id, new Point(changedNode.lon(), changedNode.lat()));
            newNode.computeReferences(access, storage);
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;

            Way newWay = new Way(id, changedWay.tags(), changedWay.version(), visible, changedWay.refs().toLongArray());
            storage.ways().put(access, id, newWay);
            newWay.computeReferences(access, storage);
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;

            Relation.Member[] members = changedRelation.members().stream().map(Relation.Member::new).toArray(Relation.Member[]::new);
            Relation newRelation = new Relation(id, changedRelation.tags(), changedRelation.version(), visible, members);
            storage.relations().put(access, id, newRelation);
            newRelation.computeReferences(access, storage);
        }

        //this will force the element's tile intersections and references to be re-computed
        changedIds.add(combinedId);
    }

    private void modify(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        int type = element.type();
        long id = element.id();
        long combinedId = Element.addTypeToId(type, id);

        Element oldElement = storage.getElement(access, combinedId);
        if (oldElement == null) {
            if (false) { //TODO: decide whether or not to keep this
                logger.warn("attempting to modify non-existent %s with id %d, assuming that it's newly re-created!\nold element: %s\nelement: %s", Element.typeName(type), id, oldElement, element);
                this.create(storage, access, element, changedIds);
                return;
            } else {
                logger.alert("attempting to modify non-existent %s with id %d!\nold element: %s\nelement: %s", Element.typeName(type), id, oldElement, element);
                throw new UpdateImpossibleException();
            }
        } else if (!oldElement.visible()) {
            if (false) {
                logger.alert("attempting to modify deleted %s with id %d!\nold element: %s\nelement: %s", Element.typeName(type), id, oldElement, element);
                throw new UpdateImpossibleException();
            } else {
                logger.warn("modified previously deleted %s with id %d, it will now be marked as visible again!\nold element: %s\nelement: %s", Element.typeName(type), id, oldElement, element);
            }
        } else if (element.version() != oldElement.version() + 1) {
            logger.alert("attempting to modify %s with id %d from version %d to %d!\nold element: %s\nelement: %s", Element.typeName(type), id, oldElement.version(), element.version(), oldElement, element);
            throw new UpdateImpossibleException();
        } else { //make sure that every element referenced by this element still knows that it's being referenced
            LongList oldReferences = oldElement.getReferencesCombinedIds();
            List<LongList> storedReferencesToElementsWeReference = storage.references().getReferencesTo(access, oldReferences);
            LongList oldReferencesWhichAreNoLongerReferenced = new LongArrayList();
            for (int i = 0; i < oldReferences.size(); i++) {
                long oldReference = oldReferences.getLong(i);
                if (!storedReferencesToElementsWeReference.get(i).contains(combinedId)) {
                    oldReferencesWhichAreNoLongerReferenced.add(oldReference);
                }
            }
            if (!oldReferencesWhichAreNoLongerReferenced.isEmpty()) {
                logger.alert("attempting to modify %s with id %d, but some elements it references are not actually referenced: %s!\nold element: %s\nelement: %s",
                        Element.typeName(type), id, oldReferencesWhichAreNoLongerReferenced, oldElement, element);
                throw new UpdateImpossibleException();
            }
        }

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;
            Node oldNode = (Node) oldElement;

            if (!oldNode.visible()) {
                Point oldPoint = storage.points().get(access, id);
                if (oldPoint != null) {
                    logger.alert("while attempting to modify deleted node with id %d: node was marked as deleted, but still has an associated point!\nold element: %s\nold point: %s\nelement: %s", id, oldElement, oldPoint, element);
                    throw new UpdateImpossibleException();
                }
            }

            storage.points().put(access, id, new Point(changedNode.lon(), changedNode.lat()));
            storage.nodes().put(access, id, new Node(id, changedNode.tags(), changedNode.version(), true));
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;
            Way oldWay = (Way) oldElement;

            LongSet oldNodes = new LongOpenHashSet(oldWay.nodes());
            LongSet newNodes = new LongOpenHashSet(changedWay.refs());
            for (long node : newNodes) {
                if (!oldNodes.remove(node)) { //node was newly added to this way
                    storage.references().addReference(access, Element.addTypeToId(Node.TYPE, node), combinedId);
                }
            }
            for (long node : oldNodes) { //all nodes that remain are no longer referenced
                storage.references().deleteReference(access, Element.addTypeToId(Node.TYPE, node), combinedId);
            }

            storage.ways().put(access, id, new Way(id, changedWay.tags(), changedWay.version(), true, changedWay.refs().toLongArray()));
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;
            Relation oldRelation = (Relation) oldElement;

            Relation.Member[] newMembers = changedRelation.members().stream().map(Relation.Member::new).toArray(Relation.Member[]::new);

            LongSet oldRefs = new LongOpenHashSet(Stream.of(oldRelation.members()).mapToLong(Relation.Member::combinedId).toArray());
            LongSet newRefs = new LongOpenHashSet(Stream.of(newMembers).mapToLong(Relation.Member::combinedId).toArray());
            for (long ref : newRefs) {
                if (!oldRefs.remove(ref)) { //element was newly added to this relation
                    storage.references().addReference(access, ref, combinedId);
                }
            }
            for (long ref : oldRefs) { //all elements that remain are no longer referenced
                storage.references().deleteReference(access, ref, combinedId);
            }

            storage.relations().put(access, id, new Relation(id, changedRelation.tags(), changedRelation.version(), true, newMembers));
        }

        //this will force the element's tile intersections and references to be re-computed
        this.markDirty(storage, access, changedIds, combinedId);
    }

    private void delete(Storage storage, DBAccess access, Changeset.Element element, LongSet changedIds) throws Exception {
        int type = element.type();
        long id = element.id();
        long combinedId = Element.addTypeToId(type, id);

        Element oldElement = storage.getElement(access, combinedId);
        if (oldElement == null) {
            logger.alert("attempting to delete non-existent %s with id %d!\nold element: %s\nelement: %s", Element.typeName(type), id, oldElement, element);
            throw new UpdateImpossibleException();
        } else { //make sure that every element referenced by this element still knows that it's being referenced
            LongList oldReferences = oldElement.getReferencesCombinedIds();
            List<LongList> storedReferencesToElementsWeReference = storage.references().getReferencesTo(access, oldReferences);
            LongList oldReferencesWhichAreNoLongerReferenced = new LongArrayList();
            for (int i = 0; i < oldReferences.size(); i++) {
                long oldReference = oldReferences.getLong(i);
                if (!storedReferencesToElementsWeReference.get(i).contains(combinedId)) {
                    oldReferencesWhichAreNoLongerReferenced.add(oldReference);
                }
            }
            if (!oldReferencesWhichAreNoLongerReferenced.isEmpty()) {
                logger.alert("attempting to modify %s with id %d, but some elements it references are not actually referenced: %s!\nold element: %s\nelement: %s",
                        Element.typeName(type), id, oldReferencesWhichAreNoLongerReferenced, oldElement, element);
                throw new UpdateImpossibleException();
            }
        }

        if (element instanceof Changeset.Node) {
            Changeset.Node changedNode = (Changeset.Node) element;
            Node oldNode = (Node) oldElement;

            storage.points().delete(access, id); //delete the associated point
            storage.nodes().put(access, id, new Node(id, changedNode.tags(), changedNode.version(), false));
        } else if (element instanceof Changeset.Way) {
            Changeset.Way changedWay = (Changeset.Way) element;
            Way oldWay = (Way) oldElement;

            if (!changedWay.refs().isEmpty()) {
                logger.alert("deleted way %d still has some references!\nold element: %s\nelement: %s", id, oldElement, element);
                throw new UpdateImpossibleException();
            }

            for (long node : oldWay.nodes()) { //all nodes that remain are no longer referenced
                storage.references().deleteReference(access, Element.addTypeToId(Node.TYPE, node), combinedId);
            }

            storage.ways().put(access, id, new Way(id, changedWay.tags(), changedWay.version(), false, new long[0]));
        } else if (element instanceof Changeset.Relation) {
            Changeset.Relation changedRelation = (Changeset.Relation) element;
            Relation oldRelation = (Relation) oldElement;

            if (!changedRelation.members().isEmpty()) {
                logger.alert("deleted relation %d still has some members!\nold element: %s\nelement: %s", id, oldElement, element);
                throw new UpdateImpossibleException();
            }

            for (long ref : Stream.of(oldRelation.members()).mapToLong(Relation.Member::combinedId).toArray()) { //all elements that remain are no longer referenced
                storage.references().deleteReference(access, ref, combinedId);
            }

            storage.relations().put(access, id, new Relation(id, changedRelation.tags(), changedRelation.version(), false, new Relation.Member[0]));
        }

        //this will force the element's tile intersections and references to be re-computed
        this.markDirty(storage, access, changedIds, combinedId);
    }

    private void markDirty(Storage storage, DBAccess access, LongSet changedIds, long combinedId) throws Exception {
        if (changedIds.add(combinedId)) {
            for (long referent : storage.references().getReferencesTo(access, combinedId)) {
                this.markDirty(storage, access, changedIds, referent);
            }
        }
    }


    private static final class UpdateImpossibleException extends RuntimeException {
    }
}
