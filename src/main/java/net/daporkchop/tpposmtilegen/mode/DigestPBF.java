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

package net.daporkchop.tpposmtilegen.mode;

import com.wolt.osm.parallelpbf.ParallelBinaryParser;
import com.wolt.osm.parallelpbf.blob.BlobInformation;
import com.wolt.osm.parallelpbf.entity.Header;
import com.wolt.osm.parallelpbf.entity.OsmEntity;
import lombok.NonNull;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.function.exception.ERunnable;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.threadlocal.TL;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.natives.OSMDataUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.natives.UInt64SetUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.CloseableThreadFactory;
import net.daporkchop.tpposmtilegen.util.IterableThreadLocal;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class DigestPBF implements IMode {
    private static <T> T getHeader(@NonNull Path path, @NonNull Function<Header, T> mapper) throws IOException {
        Header[] out = new Header[1];
        try (InputStream is = Files.newInputStream(path)) {
            new ParallelBinaryParser(is, 1)
                    .onHeader(header -> out[0] = header)
                    .parse();
        }
        return mapper.apply(Objects.requireNonNull(out[0], "header"));
    }

    private static void eraseBase(OsmEntity entity) {
        entity.getTags().clear();
        entity.setTags(null);
        entity.setInfo(null);
    }

    private static void erase(com.wolt.osm.parallelpbf.entity.Node node) {
        eraseBase(node);
    }

    private static void erase(com.wolt.osm.parallelpbf.entity.Way way) {
        eraseBase(way);
        way.getNodes().clear();
        way.setNodes(null);
    }

    private static void erase(com.wolt.osm.parallelpbf.entity.Relation relation) {
        eraseBase(relation);
        relation.getMembers().clear();
        relation.setMembers(null);
    }

    private static void eraseAuto(OsmEntity entity) {
        if (entity instanceof com.wolt.osm.parallelpbf.entity.Node) {
            erase((com.wolt.osm.parallelpbf.entity.Node) entity);
        } else if (entity instanceof com.wolt.osm.parallelpbf.entity.Way) {
            erase((com.wolt.osm.parallelpbf.entity.Way) entity);
        } else if (entity instanceof com.wolt.osm.parallelpbf.entity.Relation) {
            erase((com.wolt.osm.parallelpbf.entity.Relation) entity);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public String name() {
        return "digest_pbf";
    }

    @Override
    public String synopsis() {
        return "<planet-latest.osm.pbf> <index_dir>";
    }

    @Override
    public String help() {
        return "Creates a new index from a full OSM planet file.\n"
               + "Any existing OSM data will be purged before the import is started.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: digest_pbf <pbf> <index_dir>");
        Path src = PFiles.assertFileExists(Paths.get(args[0]));
        Path dst = Paths.get(args[1]);

        Header header = getHeader(src, Function.identity());
        logger.info("PBF header: " + header);
        checkArg(true || header.getReplicationSequenceNumber() != null || header.getReplicationTimestamp() != null,
                "'%s' doesn't contain a replication timestamp or sequence number!", src);

        //we need this in order to make sure that every element which exists is actually present in the db (i.e. deleted elements should have visible=false)
        checkArg(header.getRequiredFeatures().contains(Header.FEATURE_HISTORICAL_INFORMATION),
                "'%s' doesn't contain historical information! (required_features is missing '%s')", src, Header.FEATURE_HISTORICAL_INFORMATION);

        //we need this in order to fill the database efficiently
        checkArg(header.getOptionalFeatures().contains("Sort.Type_then_ID"),
                "'%s' isn't sorted by element ID! (optional_features is missing '%s')", src, "Sort.Type_then_ID");

        if (false) {
            try (Storage storage = new Storage(dst, DatabaseConfig.RO_GENERAL);
                 DBReadAccess access = storage.db().snapshot();
                 InputStream is = Files.newInputStream(src);
                 CloseableThreadFactory threadFactory = new CloseableThreadFactory("PBF parse worker");
                 ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Read PBF")
                         .slot("nodes").slot("ways").slot("relations")
                         .build()) {
                new ParallelBinaryParser(is, PorkUtil.CPU_COUNT)
                        .setThreadFactory(threadFactory)
                        .onBoundBox(bb -> logger.info("bounding box: %s", bb))
                        .onChangeset(changeset -> logger.info("changeset: %s", changeset))
                        .onNode((EConsumer<com.wolt.osm.parallelpbf.entity.Node>) in -> {
                            Node node = storage.nodes().get(access, in.getId());
                            checkState(node != null, "node %d isn't present in db!", in.getId());
                            checkState(node.version() >= in.getInfo().getVersion(), "node %d is outdated! should be at least version %d, but stored version is %d",
                                    in.getId(), in.getInfo().getVersion(), node.version());
                            if (node.version() == in.getInfo().getVersion()) {
                                checkState(node.tags().equals(in.getTags()), "node %d has incorrect tags! should be %s, but stored tags are %s",
                                        in.getId(), in.getTags(), node.tags());

                                Point point = storage.points().get(access, in.getId());
                                checkState(point != null, "node %d isn't present in db!", in.getId());
                                checkState(point.x() == in.getLonFixedPoint() && point.y() == in.getLatFixedPoint(),
                                        "point for node %d has incorrect coordinates! should be (%d,%d) but stored coordinates are (%d,%d)",
                                        in.getId(), in.getLonFixedPoint(), in.getLatFixedPoint(), point.x(), point.y());
                            }

                            notifier.step(Node.TYPE);
                        })
                        .onWay((EConsumer<com.wolt.osm.parallelpbf.entity.Way>) in -> {
                            Way way = storage.ways().get(access, in.getId());
                            checkState(way != null, "way %d isn't present in db!", in.getId());
                            checkState(way.version() >= in.getInfo().getVersion(), "way %d is outdated! should be at least version %d, but stored version is %d",
                                    in.getId(), in.getInfo().getVersion(), way.version());
                            if (way.version() == in.getInfo().getVersion()) {
                                checkState(way.tags().equals(in.getTags()), "way %d has incorrect tags! should be %s, but stored tags are %s",
                                        in.getId(), in.getTags(), way.tags());
                            }
                            notifier.step(Way.TYPE);
                        })
                        .onRelation((EConsumer<com.wolt.osm.parallelpbf.entity.Relation>) in -> {
                            Relation relation = storage.relations().get(access, in.getId());
                            checkState(relation != null, "relation %d isn't present in db!", in.getId());
                            checkState(relation.version() >= in.getInfo().getVersion(), "relation %d is outdated! should be at least version %d, but stored version is %d",
                                    in.getId(), in.getInfo().getVersion(), relation.version());
                            if (relation.version() == in.getInfo().getVersion()) {
                                checkState(relation.tags().equals(in.getTags()), "relation %d has incorrect tags! should be %s, but stored tags are %s",
                                        in.getId(), in.getTags(), relation.tags());
                            }
                            notifier.step(Relation.TYPE);
                        })
                        .parse();
            }
            return;
        }

        if (PFiles.checkDirectoryExists(dst)) {
            try (Stream<Path> stream = Files.list(dst)) {
                if (stream.findAny().isPresent()) {
                    logger.warn("destination folder already exists. proceed? [true/false]");
                    if (!Boolean.parseBoolean(new Scanner(System.in).nextLine())) {
                        logger.info("Abort.");
                        return;
                    }

                    try (Storage storage = new Storage(dst, DatabaseConfig.RW_LITE)) {
                        //purge all OSM data from the storage to ensure that we aren't writing over existing stuff
                        Purge.purge(storage, Purge.DataType.osm);
                    }
                }
            }
        }

        PFiles.ensureDirectoryExists(dst);

        try (Storage storage = new Storage(dst, DatabaseConfig.RW_BULK_LOAD)) {
            try (DBWriteAccess batch = storage.db().beginLocalBatch()) {
                //write replication information from the headers
                if (header.getReplicationSequenceNumber() != null) {
                    storage.sequenceNumberProperty().set(batch, header.getReplicationSequenceNumber().longValue());
                } else {
                    storage.sequenceNumberProperty().remove(batch);
                }

                if (header.getReplicationTimestamp() != null) {
                    storage.replicationTimestampProperty().set(batch, header.getReplicationTimestamp().longValue());
                } else {
                    storage.replicationTimestampProperty().remove(batch);
                }

                if (header.getReplicationBaseUrl() != null) {
                    storage.replicationBaseUrlProperty().set(batch, header.getReplicationBaseUrl());
                } else {
                    logger.warn("'%s' doesn't provide a replication base url, falling back to default: '%s'", src, Storage.DEFAULT_REPLICATION_BASE_URL);
                    storage.replicationBaseUrlProperty().set(batch, Storage.DEFAULT_REPLICATION_BASE_URL);
                }
            }

            final int threads = PorkUtil.CPU_COUNT;

            try (InputStream is = Files.newInputStream(src);
                 ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Read PBF")
                         .slot("nodes").slot("ways").slot("relations")
                         .build();
                 PbfElementHandler elementHandler = new PbfElementHandler(storage, notifier, threads);
                 CloseableThreadFactory threadFactory = new CloseableThreadFactory("PBF parse worker")) {
                new ParallelBinaryParser(is, threads)
                        .setThreadFactory(threadFactory)
                        .onBoundBox(bb -> logger.info("bounding box: %s", bb))
                        .onChangeset(changeset -> logger.info("changeset: %s", changeset))
                        .onNode(elementHandler.nodeHandler)
                        .onWay(elementHandler.wayHandler)
                        .onRelation(elementHandler.relationHandler)
                        .onBlobStart(elementHandler.blobStartHandler)
                        .onBlobComplete(elementHandler.blobCompleteHandler)
                        .parse();
                notifier.close();
            }

            try (TimedOperation compactPoints = new TimedOperation("Points compaction")) {
                storage.points().compact();
            }
            try (TimedOperation compactNodes = new TimedOperation("Nodes compaction")) {
                storage.nodes().compact();
            }
            try (TimedOperation compactWays = new TimedOperation("Ways compaction")) {
                storage.ways().compact();
            }
            try (TimedOperation compactRelations = new TimedOperation("Relations compaction")) {
                storage.relations().compact();
            }

            try (UInt64SetUnsortedWriteAccess referencesWriteAccess = new UInt64SetUnsortedWriteAccess(storage,
                    storage.db().internalColumnFamily(storage.references()), true, 4.266666667d);
                 ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Compute references")
                         .slot("nodes").slot("ways").slot("relations").slot("coastlines")
                         .build()) {
                LongObjConsumer<Element> func = (id, element) -> {
                    int type = element.type();
                    try {
                        element.computeReferences(referencesWriteAccess, storage);
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                    notifier.step(type);
                };

                storage.nodes().forEachParallel(storage.db().read(), func);
                storage.ways().forEachParallel(storage.db().read(), func);
                storage.relations().forEachParallel(storage.db().read(), func);
                storage.coastlines().forEachParallel(storage.db().read(), func);
            }

            try (TimedOperation compactReferences = new TimedOperation("References compaction")) {
                storage.references().compact();
            }
        }
    }

    private static final class PbfElementHandler implements AutoCloseable {
        private final IterableThreadLocal<ThreadState> state = IterableThreadLocal.of(ThreadState::new);
        private final TL<MutableInt> currentVersion = TL.initializedWith(MutableInt::new);

        private final Storage storage;

        private final OSMDataUnsortedWriteAccess nodesWriteAccess;
        private final OSMDataUnsortedWriteAccess pointsWriteAccess;
        private final OSMDataUnsortedWriteAccess waysWriteAccess;
        private final OSMDataUnsortedWriteAccess relationsWriteAccess;

        private final OSMDataUnsortedWriteAccess[] osmDataWriteAccesses;

        private final ProgressNotifier notifier;

        private final EConsumer<com.wolt.osm.parallelpbf.entity.Node> nodeHandler = in -> this.state.get().accept(in);
        private final EConsumer<com.wolt.osm.parallelpbf.entity.Way> wayHandler = in -> this.state.get().accept(in);
        private final EConsumer<com.wolt.osm.parallelpbf.entity.Relation> relationHandler = in -> this.state.get().accept(in);

        private final EConsumer<BlobInformation> blobStartHandler = in -> this.state.get().onBlobStart(in);
        private final ERunnable blobCompleteHandler = () -> this.state.get().onBlobComplete();

        public PbfElementHandler(@NonNull Storage storage, @NonNull ProgressNotifier notifier, int threads) throws Exception {
            this.storage = storage;

            ToIntFunction<ByteBuffer> currentVersionExtractor = buf -> this.currentVersion.get().intValue();

            this.nodesWriteAccess = new OSMDataUnsortedWriteAccess(
                    storage, storage.db().internalColumnFamily(storage.nodes()), currentVersionExtractor, 6.394704777d, threads, true);
            this.pointsWriteAccess = new OSMDataUnsortedWriteAccess(
                    storage, storage.db().internalColumnFamily(storage.points()), currentVersionExtractor, 2.154728129d, threads, true);
            this.waysWriteAccess = new OSMDataUnsortedWriteAccess(
                    storage, storage.db().internalColumnFamily(storage.ways()), currentVersionExtractor, 3.243015087d, threads, true);
            this.relationsWriteAccess = new OSMDataUnsortedWriteAccess(
                    storage, storage.db().internalColumnFamily(storage.relations()), currentVersionExtractor, 3.519971471d, threads, true);

            this.osmDataWriteAccesses = new OSMDataUnsortedWriteAccess[] {
                    this.nodesWriteAccess,
                    this.pointsWriteAccess,
                    this.waysWriteAccess,
                    this.relationsWriteAccess
            };

            this.notifier = notifier;
        }

        private void writeNode(com.wolt.osm.parallelpbf.entity.Node in) throws Exception {
            this.notifier.step(Node.TYPE);

            this.currentVersion.get().setValue(in.getInfo().getVersion());

            Node node = new Node(in);
            this.storage.nodes().put(this.nodesWriteAccess, node.id(), node);
            if (node.visible()) { //only store the point if the node is actually visible
                this.storage.points().put(this.pointsWriteAccess, node.id(), new Point(in.getLonFixedPoint(), in.getLatFixedPoint()));
            } else { //we need to explicitly delete the point in order to replace any older versions of the point for which the node was visible
                this.storage.points().delete(this.pointsWriteAccess, node.id());
            }
            node.erase();
        }

        private void writeWay(com.wolt.osm.parallelpbf.entity.Way in) throws Exception {
            this.notifier.step(Way.TYPE);

            this.currentVersion.get().setValue(in.getInfo().getVersion());

            Way way = new Way(in);
            this.storage.putWay(this.waysWriteAccess, way);
            way.erase();
        }

        private void writeRelation(com.wolt.osm.parallelpbf.entity.Relation in) throws Exception {
            this.notifier.step(Relation.TYPE);

            this.currentVersion.get().setValue(in.getInfo().getVersion());

            Relation relation = new Relation(in);
            this.storage.putRelation(this.relationsWriteAccess, relation);
            relation.erase();
        }

        @Override
        public void close() throws Exception {
            this.notifier.close();

            Stream.of(this.osmDataWriteAccesses).parallel().forEach((EConsumer<OSMDataUnsortedWriteAccess>) OSMDataUnsortedWriteAccess::flush);

            for (OSMDataUnsortedWriteAccess access : this.osmDataWriteAccesses) {
                access.close();
            }
        }

        private final class ThreadState {
            private boolean joined = false;

            //only one of these three fields may be non-null at a time
            private com.wolt.osm.parallelpbf.entity.Node node;
            private com.wolt.osm.parallelpbf.entity.Way way;
            private com.wolt.osm.parallelpbf.entity.Relation relation;

            private Class<? extends OsmEntity> lastType = com.wolt.osm.parallelpbf.entity.Node.class;

            public void accept(@NonNull com.wolt.osm.parallelpbf.entity.Node node) throws Exception {
                checkState(this.way == null && this.relation == null);
                checkState(this.lastType == com.wolt.osm.parallelpbf.entity.Node.class, this.lastType);

                if (!this.joined) {
                    this.joined = true;
                    PbfElementHandler.this.nodesWriteAccess.threadJoin();
                    PbfElementHandler.this.pointsWriteAccess.threadJoin();
                }

                if (this.node != null) {
                    if (this.node.getId() == node.getId()) {
                        checkState(this.node.getInfo().getVersion() < node.getInfo().getVersion(), "node %d goes from version %d to %d",
                                node.getId(), this.node.getInfo().getVersion(), node.getInfo().getVersion());

                        //replace the node with the new one
                    } else {
                        checkState(this.node.getId() < node.getId(), "nodes out-of-order: %d to %d", this.node.getId(), node.getId());

                        //write out the current node before advancing to the next one
                        PbfElementHandler.this.writeNode(this.node);
                    }

                    erase(this.node);
                } else {
                    //store the first node
                }
                this.node = node;
            }

            public void accept(@NonNull com.wolt.osm.parallelpbf.entity.Way way) throws Exception {
                checkState(this.node == null && this.relation == null);

                if (this.lastType == com.wolt.osm.parallelpbf.entity.Node.class) {
                    if (this.joined) {
                        PbfElementHandler.this.nodesWriteAccess.threadRemove();
                        PbfElementHandler.this.pointsWriteAccess.threadRemove();
                    }
                    PbfElementHandler.this.nodesWriteAccess.threadQuit();
                    PbfElementHandler.this.pointsWriteAccess.threadQuit();
                    this.lastType = com.wolt.osm.parallelpbf.entity.Way.class;
                }
                checkState(this.lastType == com.wolt.osm.parallelpbf.entity.Way.class, this.lastType);

                if (!this.joined) {
                    this.joined = true;
                    PbfElementHandler.this.waysWriteAccess.threadJoin();
                }

                if (this.way != null) {
                    if (this.way.getId() == way.getId()) {
                        checkState(this.way.getInfo().getVersion() < way.getInfo().getVersion(), "way %d goes from version %d to %d",
                                way.getId(), this.way.getInfo().getVersion(), way.getInfo().getVersion());

                        //replace the way with the new one
                    } else {
                        checkState(this.way.getId() < way.getId(), "ways out-of-order: %d to %d", this.way.getId(), way.getId());

                        //write out the current way before advancing to the next one
                        PbfElementHandler.this.writeWay(this.way);
                    }

                    erase(this.way);
                } else {
                    //store the first way
                }
                this.way = way;
            }

            public void accept(@NonNull com.wolt.osm.parallelpbf.entity.Relation relation) throws Exception {
                checkState(this.node == null && this.way == null);

                if (this.lastType == com.wolt.osm.parallelpbf.entity.Node.class) {
                    if (this.joined) {
                        PbfElementHandler.this.nodesWriteAccess.threadRemove();
                        PbfElementHandler.this.pointsWriteAccess.threadRemove();
                    }
                    PbfElementHandler.this.nodesWriteAccess.threadQuit();
                    PbfElementHandler.this.pointsWriteAccess.threadQuit();
                    this.lastType = com.wolt.osm.parallelpbf.entity.Way.class;
                }
                if (this.lastType == com.wolt.osm.parallelpbf.entity.Way.class) {
                    if (this.joined) {
                        PbfElementHandler.this.waysWriteAccess.threadRemove();
                    }
                    PbfElementHandler.this.waysWriteAccess.threadQuit();
                    this.lastType = com.wolt.osm.parallelpbf.entity.Relation.class;
                }
                checkState(this.lastType == com.wolt.osm.parallelpbf.entity.Relation.class, this.lastType);

                if (!this.joined) {
                    this.joined = true;
                    PbfElementHandler.this.relationsWriteAccess.threadJoin();
                }

                if (this.relation != null) {
                    if (this.relation.getId() == relation.getId()) {
                        checkState(this.relation.getInfo().getVersion() < relation.getInfo().getVersion(), "relation %d goes from version %d to %d",
                                relation.getId(), this.relation.getInfo().getVersion(), relation.getInfo().getVersion());

                        //replace the relation with the new one
                    } else {
                        checkState(this.relation.getId() < relation.getId(), "relations out-of-order: %d to %d", this.relation.getId(), relation.getId());

                        //write out the current relation before advancing to the next one
                        PbfElementHandler.this.writeRelation(this.relation);
                    }

                    erase(this.relation);
                } else {
                    //store the first relation
                }
                this.relation = relation;
            }

            public void flush() throws Exception {
                if (this.node != null) {
                    checkState(this.way == null && this.relation == null);
                    PbfElementHandler.this.writeNode(this.node);
                    erase(this.node);
                    this.node = null;
                } else if (this.way != null) {
                    checkState(this.relation == null);
                    PbfElementHandler.this.writeWay(this.way);
                    erase(this.way);
                    this.way = null;
                } else if (this.relation != null) {
                    PbfElementHandler.this.writeRelation(this.relation);
                    erase(this.relation);
                    this.relation = null;
                }
            }

            public void onBlobStart(BlobInformation blobInformation) throws Exception {
                checkState(!this.joined);
            }

            public void onBlobComplete() throws Exception {
                if (this.joined) {
                    this.flush();
                    if (this.lastType == com.wolt.osm.parallelpbf.entity.Node.class) {
                        PbfElementHandler.this.nodesWriteAccess.threadRemove();
                        PbfElementHandler.this.pointsWriteAccess.threadRemove();
                    }
                    if (this.lastType == com.wolt.osm.parallelpbf.entity.Way.class) {
                        PbfElementHandler.this.waysWriteAccess.threadRemove();
                    }
                    if (this.lastType == com.wolt.osm.parallelpbf.entity.Relation.class) {
                        PbfElementHandler.this.relationsWriteAccess.threadRemove();
                    }
                    this.joined = false;
                }
            }
        }
    }
}
