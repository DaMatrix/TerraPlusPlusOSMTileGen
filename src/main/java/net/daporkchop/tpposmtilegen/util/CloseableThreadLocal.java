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

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.throwing.EConsumer;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * Implementation of {@link FastThreadLocal} which is able to close resources when the thread exits.
 *
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public abstract class CloseableThreadLocal<V extends AutoCloseable> extends FastThreadLocal<V> implements AutoCloseable {
    public static <V extends AutoCloseable> CloseableThreadLocal<V> of(@NonNull Callable<V> factory) {
        return new CloseableThreadLocal<V>() {
            @Override
            protected V initialValue0() throws Exception {
                return factory.call();
            }
        };
    }

    protected final Map<V, Thread> instances = new IdentityHashMap<>();

    @Override
    protected V initialValue() throws Exception {
        Thread currentThread = Thread.currentThread();
        if (!(currentThread instanceof FastThreadLocalThread)) {
            logger.warn("Not a FastThreadLocalThread: %s", Thread.currentThread());
        }

        try {
            V value = this.initialValue0();
            Objects.requireNonNull(value, "initialValue0 returned null");
            synchronized (this.instances) {
                checkState(this.instances.putIfAbsent(value, Thread.currentThread()) == null, "duplicate value: %s", value);
                this.cleanup();
            }
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    protected abstract V initialValue0() throws Exception;

    @Override
    protected void onRemoval(@NonNull V value) throws Exception {
        super.onRemoval(value);

        boolean removed;
        synchronized (this.instances) {
            removed = this.instances.remove(value, Thread.currentThread());
            this.cleanup();
        }
        if (removed) {
            value.close();
        }
    }

    public void forEach(@NonNull Consumer<V> callback) throws Exception {
        synchronized (this.instances) {
            this.cleanup();
            this.instances.keySet().forEach(callback);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this.instances) {
            this.instances.keySet().forEach((EConsumer<V>) V::close);
            this.instances.clear();
        }
    }

    private void cleanup() throws Exception {
        for (Iterator<Map.Entry<V, Thread>> itr = this.instances.entrySet().iterator(); itr.hasNext(); ) {
            Map.Entry<V, Thread> entry = itr.next();
            if (!entry.getValue().isAlive()) {
                entry.getKey().close();
                itr.remove();
            }
        }
    }
}
