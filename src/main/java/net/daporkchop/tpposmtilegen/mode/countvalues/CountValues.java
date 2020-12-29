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

package net.daporkchop.tpposmtilegen.mode.countvalues;

import com.wolt.osm.parallelpbf.ParallelBinaryParser;
import lombok.NonNull;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.mode.IMode;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author DaPorkchop_
 */
public class CountValues implements IMode {
    /*
     * com.wolt.osm.parallelpbf.entity.Relation: 8364019
     * com.wolt.osm.parallelpbf.entity.BoundBox: 1
     * com.wolt.osm.parallelpbf.entity.Header: 1
     * com.wolt.osm.parallelpbf.entity.Node: 6482558025
     * com.wolt.osm.parallelpbf.entity.Way: 716217853
     */

    @Override
    public void run(@NonNull String... args) throws Exception {
        Map<Class<?>, LongAdder> counters = new ConcurrentHashMap<>();
        Function<Class<?>, LongAdder> computeFunc = c -> new LongAdder();

        Consumer callback = v -> counters.computeIfAbsent(v.getClass(), computeFunc).increment();

        try (InputStream in = new FileInputStream(args[0])) {
            new ParallelBinaryParser(in, PorkUtil.CPU_COUNT)
                    .onBoundBox(callback)
                    .onChangeset(callback)
                    .onHeader(callback)
                    .onNode(callback)
                    .onRelation(callback)
                    .onWay(callback)
                    .parse();
        }

        counters.forEach((clazz, count) -> System.out.printf("%s: %d\n", clazz.getCanonicalName(), count.sum()));
    }
}
