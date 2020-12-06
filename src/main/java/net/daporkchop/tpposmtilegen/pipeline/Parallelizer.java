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

package net.daporkchop.tpposmtilegen.pipeline;

import lombok.NonNull;
import net.daporkchop.lib.common.misc.threadfactory.PThreadFactories;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.BetterBlockingQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author DaPorkchop_
 */
public class Parallelizer<T> extends FilterPipelineStep<T, T> {
    protected final ExecutorService executor;

    protected final IOException exception = new IOException();
    protected boolean isException = false;

    public Parallelizer(@NonNull PipelineStep<T> next) {
        this(next, 512, PorkUtil.CPU_COUNT - 1);
    }

    public Parallelizer(@NonNull PipelineStep<T> next, int maxQueueSize, int threads) {
        super(next);
        this.executor = new ThreadPoolExecutor(threads, threads,
                1L, TimeUnit.SECONDS,
                new BetterBlockingQueue<>(maxQueueSize),
                PThreadFactories.builder().daemon().build());
    }

    @Override
    public void accept(T value) throws IOException {
        if (this.isException) {
            throw new RuntimeException("concurrent execution exception");
        }

        this.executor.execute(() -> {
            try {
                this.next.accept(value);
            } catch (IOException e) {
                synchronized (this.exception) {
                    this.exception.addSuppressed(e);
                }
                this.isException = true;
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            System.out.println("Waiting for async tasks to be completed...");
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (this.isException) {
                throw this.exception;
            }
        } finally {
            this.next.close();
        }
    }
}
