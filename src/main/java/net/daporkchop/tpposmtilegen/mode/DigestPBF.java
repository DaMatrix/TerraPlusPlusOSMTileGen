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
import com.wolt.osm.parallelpbf.entity.Header;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IORunnable;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.natives.UInt64SetUnsortedWriteAccess;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.util.CloseableThreadFactory;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.TimedOperation;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;

import java.io.File;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class DigestPBF implements IMode {
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
        File src = PFiles.assertFileExists(new File(args[0]));
        File dst = new File(args[1]);

        if (PFiles.checkDirectoryExists(dst)) {
            try (Stream<Path> stream = Files.list(dst.toPath())) {
                if (stream.findAny().isPresent()) {
                    logger.warn("destination folder already exists. proceed? [true/false]");
                    if (!Boolean.parseBoolean(new Scanner(System.in).nextLine())) {
                        logger.info("Abort.");
                        return;
                    }
                }
            }
        }

        PFiles.ensureDirectoryExists(dst);

        try (Storage storage = new Storage(dst.toPath(), DatabaseConfig.RW_BULK_LOAD)) {
            //purge all OSM data from the storage to ensure that we aren't writing over existing stuff
            Purge.purge(storage, Purge.DataType.osm);

            try (UInt64SetUnsortedWriteAccess referencesWriteAccess = new UInt64SetUnsortedWriteAccess(storage, storage.db()
                    .internalColumnFamily(storage.references()), true, 4.266666667d);
                 InputStream is = Files.newInputStream(src.toPath());
                 CloseableThreadFactory threadFactory = new CloseableThreadFactory("PBF parse worker");
                 ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Read PBF")
                         .slot("nodes").slot("ways").slot("relations")
                         .build()) {

                new ParallelBinaryParser(is, PorkUtil.CPU_COUNT)
                        .setThreadFactory(threadFactory)
                        .onHeader((EConsumer<Header>) header -> {
                            logger.info("PBF header: %s", header);
                            if (header.getReplicationSequenceNumber() == null && header.getReplicationTimestamp() == null) {
                                logger.error("\"%s\" doesn't contain a replication timestamp or sequence number!", src);
                                System.exit(1);
                            }

                            if (header.getReplicationSequenceNumber() != null) {
                                storage.sequenceNumber().set(storage.db().batch(), header.getReplicationSequenceNumber());
                            }
                            if (header.getReplicationTimestamp() != null) {
                                storage.replicationTimestamp().set(header.getReplicationTimestamp());
                            }
                            if (header.getReplicationBaseUrl() != null) {
                                storage.setReplicationBaseUrl(header.getReplicationBaseUrl());
                            }
                        })
                        .onBoundBox(bb -> logger.info("bounding box: %s", bb))
                        .onChangeset(changeset -> logger.info("changeset: %s", changeset))
                        .onNode((EConsumer<com.wolt.osm.parallelpbf.entity.Node>) in -> {
                            Node node = new Node(in);
                            storage.putNode(storage.db().sstBatch(), node, new Point(in.getLon(), in.getLat()));
                            node.computeReferences(referencesWriteAccess, storage);

                            notifier.step(Node.TYPE);
                        })
                        .onWay((EConsumer<com.wolt.osm.parallelpbf.entity.Way>) in -> {
                            Way way = new Way(in);
                            storage.putWay(storage.db().sstBatch(), way);
                            way.computeReferences(referencesWriteAccess, storage);

                            notifier.step(Way.TYPE);
                        })
                        .onRelation((EConsumer<com.wolt.osm.parallelpbf.entity.Relation>) in -> {
                            Relation relation = new Relation(in);
                            storage.putRelation(storage.db().sstBatch(), relation);
                            relation.computeReferences(referencesWriteAccess, storage);

                            notifier.step(Relation.TYPE);
                        })
                        .parse();
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
            try (TimedOperation compactReferences = new TimedOperation("References compaction")) {
                storage.references().compact();
            }
        }
    }
}
