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

import lombok.AllArgsConstructor;
import lombok.NonNull;
import net.daporkchop.lib.common.util.PorkUtil;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author DaPorkchop_
 */
@AllArgsConstructor
public class CloseableExecutor implements Executor, AutoCloseable {
    @NonNull
    protected final ExecutorService delegate;
    @NonNull
    protected final CloseableThreadFactory factory;

    public CloseableExecutor() {
        this(new CloseableThreadFactory(), PorkUtil.CPU_COUNT);
    }

    public CloseableExecutor(@NonNull String name) {
        this(new CloseableThreadFactory(name), PorkUtil.CPU_COUNT);
    }

    public CloseableExecutor(@NonNull CloseableThreadFactory factory) {
        this(factory, PorkUtil.CPU_COUNT);
    }

    public CloseableExecutor(@NonNull CloseableThreadFactory factory, int threads) {
        this(Executors.newFixedThreadPool(threads, factory), factory);
    }

    @Override
    public void execute(@NonNull Runnable command) {
        this.delegate.execute(command);
    }

    @Override
    public void close() throws Exception {
        this.delegate.shutdown();
        this.factory.close();
    }
}
