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

package net.daporkchop.tpposmtilegen.util.persistent;

import lombok.NonNull;
import net.daporkchop.lib.common.function.throwing.EConsumer;
import net.daporkchop.tpposmtilegen.util.CloseableThreadLocal;

import java.util.ArrayList;
import java.util.List;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * A {@link ForwardingPersistentMap} which can buffer individually queued writes.
 *
 * @author DaPorkchop_
 */
public final class BufferedPersistentMap<K, V> extends ForwardingPersistentMap<K, V> {
    private final CloseableThreadLocal<ThreadState> states = CloseableThreadLocal.of(ThreadState::new);
    private final int bufferCount;

    public BufferedPersistentMap(PersistentMap<K, V> delegate, int bufferCount) {
        super(delegate);
        this.bufferCount = positive(bufferCount, "bufferCount");
    }

    @Override
    public void put(@NonNull K key, @NonNull V value) throws Exception {
        this.states.get().put(key, value, this.bufferCount);
    }

    @Override
    public void flush() throws Exception {
        this.states.forEach((EConsumer<ThreadState>) ThreadState::close);

        super.flush();
    }

    @Override
    public void close() throws Exception {
        this.states.close();

        super.close();
    }

    /**
     * Stores the currently buffered entries for a single thread.
     *
     * @author DaPorkchop_
     */
    private final class ThreadState implements AutoCloseable {
        private final List<K> keys = new ArrayList<>();
        private final List<V> values = new ArrayList<>();

        private int buffered = 0;

        private void put(K key, V value, int flushThreshold) throws Exception {
            this.keys.add(key);
            this.values.add(value);

            if (++this.buffered == flushThreshold) {
                this.close();
            }
        }

        @Override
        public void close() throws Exception {
            if (this.buffered != 0) {
                this.buffered = 0;
                try {
                    BufferedPersistentMap.this.putAll(this.keys, this.values);
                } finally {
                    this.keys.clear();
                    this.values.clear();
                }
            }
        }
    }
}
