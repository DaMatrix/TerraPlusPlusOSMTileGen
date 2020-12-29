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

package net.daporkchop.tpposmtilegen.mode.testindex;

import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapBitSet;

import java.io.File;
import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class TestIndex implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        BitSet java = new BitSet();
        int max = 10000000;

        PFiles.rm(new File(args[0], "bitset"));
        try (OffHeapBitSet bitSet = new OffHeapBitSet(PFiles.ensureFileExists(new File(args[0], "bitset")).toPath(), 1L << 40L)) {
            if (true) {
                for (int i = 0; i < 10000; i++) {
                    int j = ThreadLocalRandom.current().nextInt(max);
                    bitSet.set(j);
                    java.set(j);
                }
            } else {
                for (int i = 0; i < max; i += ThreadLocalRandom.current().nextInt(4)) {
                    bitSet.set(i);
                    java.set(i);
                }
            }

            for (int i = 0; i < max; i++) {
                checkState(java.get(i) == bitSet.get(i), "bit %d (byte %d, word %d)", i, i >> 3, i >> 6);
            }
        }

        try (OffHeapBitSet bitSet = new OffHeapBitSet(new File(args[0], "bitset").toPath(), 1L << 40L)) {
            for (int i = 0; i < max; i++) {
                checkState(java.get(i) == bitSet.get(i), "bit %d (byte %d, word %d)", i, i >> 3, i >> 6);
            }
        }
    }
}
