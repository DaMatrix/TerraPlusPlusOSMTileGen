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

package net.daporkchop.tpposmtilegen.mode.assemblebvh;

import com.wolt.osm.parallelpbf.ParallelBinaryParser;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.threadfactory.PThreadFactories;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.storage.Node;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class AssembleBVH implements IMode {
    /*
     * com.wolt.osm.parallelpbf.entity.Relation: 8364019
     * com.wolt.osm.parallelpbf.entity.BoundBox: 1
     * com.wolt.osm.parallelpbf.entity.Header: 1
     * com.wolt.osm.parallelpbf.entity.Node: 6482558025
     * com.wolt.osm.parallelpbf.entity.Way: 716217853
     */

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: assemble_bvh <src> <dst>");
        File src = PFiles.assertFileExists(new File(args[0]));
        File dst = new File(args[1]);

        //TODO: remove this
        PFiles.rm(dst);

        checkArg(!PFiles.checkDirectoryExists(dst), "destination folder already exists: %s", dst);
        PFiles.ensureDirectoryExists(dst);

        try (Storage storage = new Storage(dst)) {
            try (InputStream is = new FileInputStream(src)) {
                new ParallelBinaryParser(is, PorkUtil.CPU_COUNT).setThreadFactory(PThreadFactories.DEFAULT_THREAD_FACTORY)
                        .onHeader(System.out::println).onBoundBox(System.out::println).onChangeset(System.out::println)
                        .onNode(in -> {
                            Node node = new Node(in.getId(), in.getTags().isEmpty() ? Collections.emptyMap() : in.getTags(), in.getLon(), in.getLat());

                            try {
                                storage.writeNodeDB().createNode(node);
                            } catch (SQLException e) {
                                throw new RuntimeException("unable to create node: " + node, e);
                            }
                        })
                        .parse();
            }
        }
    }
}
