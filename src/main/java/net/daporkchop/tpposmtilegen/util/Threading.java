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

package net.daporkchop.tpposmtilegen.util;

import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.misc.threadfactory.PThreadFactories;
import net.daporkchop.lib.common.util.PorkUtil;

import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * Needed because a parallel stream can't use a custom thread factory, and I want to ensure everything is running on a {@link FastThreadLocalThread}.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class Threading {
    public void forEachParallelLong(@NonNull Spliterator.OfLong spliterator, @NonNull LongConsumer callback) {
        forEachParallel(PorkUtil.CPU_COUNT, spliterator, s -> s.forEachRemaining(callback));
    }

    public void forEachParallelLong(int threads, @NonNull Spliterator.OfLong spliterator, @NonNull LongConsumer callback) {
        forEachParallel(threads, spliterator, s -> s.forEachRemaining(callback));
    }

    public <S extends Spliterator<?>> void forEachParallel(int threads, @NonNull S spliterator, @NonNull Consumer<S> callback) {
        ExecutorService executor = Executors.newFixedThreadPool(threads, PThreadFactories.DEFAULT_THREAD_FACTORY);
        try {
            long targetSize = Math.max(spliterator.estimateSize() / (PorkUtil.CPU_COUNT << 3), 1L);
            forEachParallel0(executor, targetSize, spliterator, callback).join();
        } finally {
            executor.shutdown();
        }
    }

    private <S extends Spliterator<?>> CompletableFuture<Void> forEachParallel0(ExecutorService executor, long targetSize, S spliterator, Consumer<S> callback) {
        long sizeEstimate = spliterator.estimateSize();
        S leftSplit;
        if (sizeEstimate <= targetSize || (leftSplit = uncheckedCast(spliterator.trySplit())) == null) {
            return CompletableFuture.runAsync(() -> callback.accept(spliterator), executor);
        } else {
            return CompletableFuture.allOf(
                    forEachParallel0(executor, targetSize, spliterator, callback),
                    forEachParallel0(executor, targetSize, leftSplit, callback));
        }
    }
}
