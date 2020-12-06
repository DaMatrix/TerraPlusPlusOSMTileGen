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
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.binary.oio.appendable.PAppendable;
import net.daporkchop.lib.binary.oio.writer.UTF8FileWriter;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.util.ByteArrayKey;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public class StringCounterImpl implements PipelineStep<byte[]>, Function<ByteArrayKey, LongAdder> {
    protected final Map<ByteArrayKey, LongAdder> counts = new ConcurrentHashMap<>();

    @NonNull
    protected final File dstFile;

    @Override
    public void accept(@NonNull byte[] value) throws IOException {
        if (value.length > 32) {
            return; //don't bother indexing long strings
        }

        this.counts.computeIfAbsent(new ByteArrayKey(value), this).increment();
    }

    @Override
    public void close() throws IOException {
        System.out.println("Sorting...");
        Map<Long, List<byte[]>> sorted = this.counts.entrySet().stream().collect(Collectors.groupingBy(
                e -> e.getValue().sum(),
                () -> new TreeMap<Long, List<byte[]>>(Comparator.reverseOrder()),
                Collectors.mapping(e -> e.getKey().value(), Collectors.toList())));
        System.out.println("Writing to file");
        try (DataOut out = DataOut.wrap(this.dstFile)) {
            sorted.forEach((count, strings) -> strings.forEach((IOConsumer<byte[]>) string -> {
                out.writeVarInt(string.length);
                out.write(string);
                out.writeVarLong(count);
            }));
            out.writeVarInt(-1);
        }
    }

    /**
     * @deprecated internal API, do not touch!
     */
    @Override
    @Deprecated
    public LongAdder apply(ByteArrayKey s) {
        return new LongAdder();
    }
}
