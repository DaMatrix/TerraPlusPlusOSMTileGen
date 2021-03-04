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

import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.util.Threading;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.stream.LongStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class GenerateBlankTiles implements IMode {
    @Override
    public String name() {
        return "generate_blank_tiles";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "Generates empty tile JSON files for tiles that contain no elements.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: generate_blank_tiles <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        try (Storage storage = new Storage(src.toPath())) {
            int X = 180 * 64;
            int Z = 90 * 64;

            Threading.forEachParallelLong(x -> {
                try (DBAccess txn = storage.db().newTransaction()) {
                    for (int z = -Z; z <= Z; z++) {
                        String path = PStrings.fastFormat("0/tile/%d/%d.json", x, z);
                        if (storage.files().get(txn, path) == null) {
                            storage.files().putHeap(txn, path, ByteBuffer.allocate(0));
                        }
                    }
                    txn.flush(true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, LongStream.rangeClosed(-X, X).spliterator());
        }
    }
}
