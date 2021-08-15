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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import static java.lang.Math.*;

/**
 * A 2-dimensional bounding box.
 *
 * @author DaPorkchop_
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Setter
@ToString
public final class Bounds2d {
    public static Bounds2d of(int x0, int x1, int y0, int y1) {
        return new Bounds2d(min(x0, x1), max(x0, x1), min(y0, y1), max(y0, y1));
    }

    private final int minX;
    private final int maxX;
    private final int minY;
    private final int maxY;

    public boolean contains(@NonNull Bounds2d other) {
        return this.contains(other.minX(), other.maxX(), other.minY(), other.maxY());
    }

    public boolean contains(int minX, int maxX, int minY, int maxY) {
        return this.minX <= minX && this.maxX >= maxX && this.minY <= minY && this.maxY >= maxY;
    }

    public boolean intersects(@NonNull Bounds2d other) {
        return this.intersects(other.minX(), other.maxX(), other.minY(), other.maxY());
    }

    public boolean intersects(int minX, int maxX, int minY, int maxY) {
        return this.minX <= maxX && this.maxX >= minX && this.minY <= maxY && this.maxY >= minY;
    }

    public Bounds2d union(@NonNull Bounds2d other) {
        if (other.contains(this)) {
            return other;
        }
        return this.union(other.minX(), other.maxX(), other.minY(), other.maxY());
    }

    public Bounds2d union(int minX, int maxX, int minY, int maxY) {
        if (this.contains(minX, maxX, minY, maxY)) {
            return this;
        }

        return of(min(this.minX, minX), max(this.maxX, maxX), min(this.minY, minY), max(this.maxY, maxY));
    }
}
