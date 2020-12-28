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

package net.daporkchop.tpposmtilegen.util;

import lombok.NonNull;

import java.util.ArrayDeque;
import java.util.Objects;

/**
 * A simple, single-threaded object instance recycler.
 *
 * @author DaPorkchop_
 */
public abstract class SimpleRecycler<T> extends ArrayDeque<T> {
    /**
     * @return a value from this recycler
     */
    public T get() {
        T value = this.pollLast();
        return value != null ? value : Objects.requireNonNull(this.newInstance0());
    }

    protected abstract T newInstance0();

    /**
     * Releases a value, allowing it to be re-used by this recycler.
     *
     * @param value the value to release
     */
    public void release(@NonNull T value) {
        this.reset0(value);
        this.addLast(value);
    }

    protected abstract void reset0(@NonNull T value);
}
