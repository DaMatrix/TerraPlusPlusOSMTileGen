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

package net.daporkchop.tpposmtilegen.mode.countstrings;

import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.util.cstring;
import net.daporkchop.tpposmtilegen.util.offheap.OffHeapString2LongTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class StringCounterImpl implements PipelineStep<byte[]> {
    protected final OffHeapString2LongTreeMap counts;

    protected final File dstFile;

    public StringCounterImpl(@NonNull File dstFile) throws IOException {
        this.dstFile = dstFile;
        this.counts = new OffHeapString2LongTreeMap(dstFile.toPath());
    }

    @Override
    public void accept(@NonNull byte[] value) throws IOException {
        if (value.length > 32) {
            return; //don't bother indexing long strings
        }

        long addr = PUnsafe.allocateMemory(value.length + 1);
        PUnsafe.copyMemory(value, PUnsafe.ARRAY_BYTE_BASE_OFFSET, null, addr, value.length);
        PUnsafe.putByte(addr + value.length, (byte) 0);

        synchronized (this.counts) {
            this.counts.increment(addr);
        }

        PUnsafe.freeMemory(addr);
    }

    @Override
    public void close() throws IOException {
        System.out.println("Sorting...");
        Map<Long, List<byte[]>> sorted = new TreeMap<>();

        this.counts.forEach((keyAddr, value) -> {
            int keyLen = toInt(cstring.strlen(keyAddr));
            byte[] key = new byte[keyLen];
            PUnsafe.copyMemory(null, keyAddr, key, PUnsafe.ARRAY_BYTE_BASE_OFFSET, keyLen);

            sorted.computeIfAbsent(value, i -> new ArrayList<>()).add(key);
        });
        this.counts.close();

        sorted.forEach((cnt, strings) -> strings.forEach((IOConsumer<byte[]>) string -> {
            System.out.write(string);
            System.out.println(" " + cnt);
        }));
    }
}
