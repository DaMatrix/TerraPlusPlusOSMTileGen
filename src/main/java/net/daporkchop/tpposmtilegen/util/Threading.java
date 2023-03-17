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

package net.daporkchop.tpposmtilegen.util;

import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.function.exception.EConsumer;
import net.daporkchop.lib.common.util.PorkUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
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
    public void forEachParallelLong(@NonNull LongConsumer callback, @NonNull Spliterator.OfLong... spliterators) throws Exception {
        forEachParallel(PorkUtil.CPU_COUNT, s -> s.forEachRemaining(callback), spliterators);
    }

    public void forEachParallelLong(int threads, @NonNull LongConsumer callback, @NonNull Spliterator.OfLong... spliterators) throws Exception {
        forEachParallel(threads, s -> s.forEachRemaining(callback), spliterators);
    }

    public <S extends Spliterator<?>> void forEachParallel(int threads, @NonNull Consumer<S> callback, @NonNull S... spliterators) throws Exception {
        if (Thread.currentThread() instanceof ForkJoinWorkerThread) {
            forEachParallel(((ForkJoinWorkerThread) Thread.currentThread()).getPool(), threads, callback, spliterators);
        } else {
            try (CloseableExecutor executor = new CloseableExecutor(new CloseableThreadFactory(), threads)) {
                forEachParallel(executor, threads, callback, spliterators);
            }
        }
    }

    public <S extends Spliterator<?>> void forEachParallel(@NonNull Executor executor, int threads, @NonNull Consumer<S> callback, @NonNull S... spliterators) throws Exception {
        CompletableFuture.allOf(Arrays.stream(spliterators)
                        .map(spliterator -> {
                            long targetSize = Math.max(spliterator.estimateSize() / ((long) threads << 2), 1L);
                            return forEachParallel0(executor, targetSize, spliterator, callback);
                        })
                        .toArray(CompletableFuture[]::new))
                .join();
    }

    private <S extends Spliterator<?>> CompletableFuture<Void> forEachParallel0(Executor executor, long targetSize, S spliterator, Consumer<S> callback) {
        long sizeEstimate = spliterator.estimateSize();
        S leftSplit;
        if (sizeEstimate <= targetSize || (leftSplit = uncheckedCast(spliterator.trySplit())) == null) {
            return CompletableFuture.runAsync(() -> callback.accept(spliterator), executor);
        } else {
            return CompletableFuture.completedFuture(null)
                    .thenComposeAsync(unused -> CompletableFuture.allOf(
                            forEachParallel0(executor, targetSize, spliterator, callback),
                            forEachParallel0(executor, targetSize, leftSplit, callback)), executor);
        }
    }

    public <T> void iterateParallel(int maxQueueSize, @NonNull EConsumer<EConsumer<T>> iterator, @NonNull EConsumer<T> function) throws Exception {
        iterateParallel(PorkUtil.CPU_COUNT, maxQueueSize, iterator, function);
    }

    public <T> void iterateParallel(int threads, int maxQueueSize, @NonNull EConsumer<EConsumer<T>> iterator, @NonNull EConsumer<T> function) throws Exception {
        if (Thread.currentThread() instanceof ForkJoinWorkerThread) {
            iterateParallel(((ForkJoinWorkerThread) Thread.currentThread()).getPool(), maxQueueSize, iterator, function);
        } else {
            try (CloseableExecutor executor = new CloseableExecutor(new CloseableThreadFactory(), threads)) {
                iterateParallel(executor, maxQueueSize, iterator, function);
            }
        }
    }

    public <T> void iterateParallel(@NonNull Executor executor, int maxQueueSize, @NonNull EConsumer<EConsumer<T>> iterator, @NonNull EConsumer<T> function) throws Exception {
        List<Throwable> exceptions = new ArrayList<>();
        try {
            Semaphore semaphore = new Semaphore(maxQueueSize);
            iterator.acceptThrowing(v -> {
                tryThrowExceptions(exceptions);

                semaphore.acquireUninterruptibly();
                executor.execute(() -> {
                    try {
                        if (exceptions.isEmpty()) {
                            function.accept(v);
                        }
                    } catch (Throwable t) {
                        synchronized (exceptions) {
                            exceptions.add(t);
                        }
                    } finally {
                        semaphore.release();
                    }
                });
            });
        } catch (Exception e) {
            tryAddExceptions(exceptions, e);
            throw e;
        }
        tryThrowExceptions(exceptions);
    }

    @SneakyThrows
    private void tryThrowExceptions(List<Throwable> exceptions) {
        if (!exceptions.isEmpty()) {
            AsyncException e = new AsyncException();
            tryAddExceptions(exceptions, e);
            throw e;
        }
    }

    private void tryAddExceptions(List<Throwable> exceptions, Throwable e) {
        synchronized (exceptions) {
            exceptions.forEach(e::addSuppressed);
            exceptions.clear();
        }
    }

    public static class AsyncException extends Exception {
    }
}
