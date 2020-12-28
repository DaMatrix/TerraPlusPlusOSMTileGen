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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import net.daporkchop.lib.common.misc.Cloneable;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;

/**
 * A 2-dimensional double-precision floating-point bounding box.
 *
 * @author DaPorkchop_
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Setter
@ToString
public final class Bounds2d implements AutoCloseable, Cloneable<Bounds2d> {
    private static final Ref<Recycler> RECYCLER = ThreadRef.soft(Recycler::new);

    public static Bounds2d blank() {
        return RECYCLER.get().get();
    }

    public static Bounds2d of(double minX, double maxX, double minZ, double maxZ) {
        return blank().minX(minX).maxX(maxX).minZ(minZ).maxZ(maxZ);
    }

    protected double minX = Double.NaN;
    protected double maxX = Double.NaN;
    protected double minZ = Double.NaN;
    protected double maxZ = Double.NaN;

    public boolean contains(@NonNull Bounds2d other) {
        return this.contains(other.minX(), other.maxX(), other.minZ(), other.maxZ());
    }

    public boolean contains(double minX, double maxX, double minZ, double maxZ) {
        return this.minX <= minX && this.maxX >= maxX && this.minZ <= minZ && this.maxZ >= maxZ;
    }

    @Override
    public void close() {
        RECYCLER.get().release(this);
    }

    @Override
    public Bounds2d clone() {
        return of(this.minX, this.maxX, this.minZ, this.maxZ);
    }

    /**
     * {@link SimpleRecycler} implementation for {@link Bounds2d} instances.
     *
     * @author DaPorkchop_
     */
    private static final class Recycler extends SimpleRecycler<Bounds2d> {
        @Override
        protected Bounds2d newInstance0() {
            return new Bounds2d();
        }

        @Override
        protected void reset0(@NonNull Bounds2d value) {
            value.minX(Double.NaN).maxX(Double.NaN).minZ(Double.NaN).maxZ(Double.NaN);
        }
    }
}
