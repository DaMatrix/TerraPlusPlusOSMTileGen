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
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public class PipelineBuilder<I, T> {
    protected final List<Function<PipelineStep, PipelineStep>> pipeline = new ArrayList<>();

    public <T1> PipelineBuilder<I, T1> first(@NonNull Function<PipelineStep<T1>, PipelineStep<I>> creator) {
        checkState(this.pipeline.isEmpty(), "first() must be called first!");
        this.pipeline.add(uncheckedCast(creator));
        return uncheckedCast(this);
    }

    public PipelineBuilder<I, T> filter(@NonNull Function<PipelineStep<T>, PipelineStep<T>> creator) {
        checkState(!this.pipeline.isEmpty(), "first() must be called first!");
        this.pipeline.add(uncheckedCast(creator));
        return uncheckedCast(this);
    }

    public <T1> PipelineBuilder<I, T1> map(@NonNull Function<PipelineStep<T1>, PipelineStep<T>> creator) {
        checkState(!this.pipeline.isEmpty(), "first() must be called first!");
        this.pipeline.add(uncheckedCast(creator));
        return uncheckedCast(this);
    }

    public PipelineStep<I> tail(@NonNull PipelineStep<?> tail) {
        PipelineStep step = tail;
        for (int i = this.pipeline.size() - 1; i >= 0; i--) {
            step = this.pipeline.get(i).apply(step);
        }
        return uncheckedCast(step);
    }
}
