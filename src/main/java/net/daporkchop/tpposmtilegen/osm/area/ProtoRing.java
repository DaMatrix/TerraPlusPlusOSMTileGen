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

package net.daporkchop.tpposmtilegen.osm.area;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;

import java.util.ArrayList;
import java.util.List;

/**
 * @author DaPorkchop_
 */
final class ProtoRing extends ObjectArrayList<Segment> {
    protected final List<ProtoRing> inner = new ArrayList<>();
    protected Segment minSegment;
    protected ProtoRing outer;

    protected long sum;

    public ProtoRing(Segment segment) {
        this.minSegment = segment;
    }

    void addSegmentBack(Segment segment) {
        if (segment.lessThan(this.minSegment)) {
            this.minSegment = segment;
        }
        this.add(segment);
        segment.ring(this);
        this.sum += segment.det();
    }

    boolean isOuter() {
        return this.outer == null;
    }

    boolean closed() {
        return this.get(0).equals(this.get(this.size() - 1));
    }

    void reverse() {
        int size = this.size();
        for (int i = 0; i < size; i++) {
            Segment segment = this.get(i);
            segment.reverse(!segment.reverse());
        }
        ObjectArrays.reverse(this.a, 0, size);
    }

    void markDirectionDone() {
        for (int i = 0, size = this.size(); i < size; i++) {
            this.get(i).directionDone(true);
        }
    }

    boolean isCw() {
        return this.sum <= 0L;
    }

    void fixDirection() {
        if (this.isCw() == this.isOuter()) {
            this.reverse();
        }
    }

    void reset() {
        this.inner.clear();
        this.outer = null;
        for (int i = 0, size = this.size(); i < size; i++) {
            this.get(i).directionDone(false);
        }
    }
}
