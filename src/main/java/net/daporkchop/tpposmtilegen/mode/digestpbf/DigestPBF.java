/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.mode.digestpbf;

import com.wolt.osm.parallelpbf.ParallelBinaryParser;
import com.wolt.osm.parallelpbf.entity.RelationMember;
import lombok.NonNull;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.threadfactory.PThreadFactories;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class DigestPBF implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: digest_pbf <src> <dst>");
        File src = PFiles.assertFileExists(new File(args[0]));
        File dst = new File(args[1]);

        checkArg(!PFiles.checkDirectoryExists(dst), "destination folder already exists: %s", dst);
        PFiles.ensureDirectoryExists(dst);

        try (ProgressNotifier notifier = new ProgressNotifier("  Read PBF: ", 5000L, "nodes", "ways", "relations");
             Storage storage = new Storage(dst.toPath());
             InputStream is = new FileInputStream(src)) {

            new ParallelBinaryParser(is, PorkUtil.CPU_COUNT)
                    .setThreadFactory(PThreadFactories.DEFAULT_THREAD_FACTORY)
                    .onHeader(header -> {
                        System.out.println(header);
                        if (header.getReplicationSequenceNumber() != null) {
                            storage.sequenceNumber().set(header.getReplicationSequenceNumber());
                        } else if (header.getReplicationTimestamp() != null) {
                            storage.replicationTimestamp().set(header.getReplicationTimestamp());
                        } else {
                            System.err.printf("\"%s\" doesn't contain a replication timestamp or sequence number!\n", src);
                            System.exit(1);
                        }
                    })
                    .onBoundBox(System.out::println).onChangeset(System.out::println)
                    .onNode((EConsumer<com.wolt.osm.parallelpbf.entity.Node>) in -> {
                        Node node = new Node(in.getId(), in.getTags().isEmpty() ? Collections.emptyMap() : in.getTags(), in.getLon(), in.getLat());
                        storage.nodes().put(in.getId(), node);
                        storage.nodeFlags().set(in.getId());

                        notifier.step(Node.TYPE);
                    })
                    .onWay((EConsumer<com.wolt.osm.parallelpbf.entity.Way>) in -> {
                        List<Long> nodesList = in.getNodes();
                        long[] nodesArray = new long[nodesList.size()];
                        for (int i = 0; i < nodesArray.length; i++) {
                            nodesArray[i] = nodesList.get(i);
                        }

                        Way way = new Way(in.getId(), in.getTags().isEmpty() ? Collections.emptyMap() : in.getTags(), nodesArray);
                        storage.ways().put(in.getId(), way);
                        storage.wayFlags().set(in.getId());

                        notifier.step(Way.TYPE);
                    })
                    .onRelation((EConsumer<com.wolt.osm.parallelpbf.entity.Relation>) in -> {
                        List<RelationMember> memberList = in.getMembers();
                        Relation.Member[] membersArray = new Relation.Member[memberList.size()];
                        for (int i = 0; i < membersArray.length; i++) {
                            membersArray[i] = new Relation.Member(memberList.get(i));
                        }

                        Relation relation = new Relation(in.getId(), in.getTags().isEmpty() ? Collections.emptyMap() : in.getTags(), membersArray);
                        storage.relations().put(in.getId(), relation);
                        storage.relationFlags().set(in.getId());

                        notifier.step(Relation.TYPE);
                    })
                    .parse();
        }
    }
}
