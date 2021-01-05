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

package net.daporkchop.tpposmtilegen.geometry;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import lombok.ToString;
import net.daporkchop.tpposmtilegen.util.Persistent;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * A closed line loop in an {@link Area}, possibly with holes.
 * <p>
 * Should probably be named {@code Polygon}, but I already have a class with that name...
 *
 * @author DaPorkchop_
 */
@ToString
public final class Shape implements Persistent {
    private static Point[] loopFromBytes(ByteBuf src) {
        int count = src.readIntLE();
        Point[] loop = new Point[count + 1];
        for (int i = 0; i < count; i++) {
            loop[i] = new Point(src);
        }
        loop[count] = loop[0];
        return loop;
    }

    protected final Point[] outerLoop;
    protected final Point[][] innerLoops;

    public Shape(@NonNull Point[] outerLoop, @NonNull Point[][] innerLoops) {
        checkArg(outerLoop.length >= 3, "outerLoop must contain at least 3 points! (found: %d)", outerLoop.length);
        for (int i = 0; i < innerLoops.length; i++) {
            Point[] innerLoop = innerLoops[i];
            checkArg(innerLoop != null, "innerLoop[%d] is null!", i);
            checkArg(innerLoop.length >= 3, "innerLoop[%d] must contain at least 3 points! (found: %d)", i, innerLoop.length);
        }

        this.outerLoop = outerLoop;
        this.innerLoops = innerLoops;
    }

    public Shape(@NonNull ByteBuf src) {
        this.outerLoop = loopFromBytes(src);
        this.innerLoops = new Point[src.readIntLE()][];
        for (int i = 0; i < this.innerLoops.length; i++) {
            this.innerLoops[i] = loopFromBytes(src);
        }
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        this.loopToBytes(dst, this.outerLoop);
        dst.writeIntLE(this.innerLoops.length);
        for (Point[] loop : this.innerLoops) {
            this.loopToBytes(dst, loop);
        }
    }

    private void loopToBytes(ByteBuf dst, Point[] loop) {
        int count = loop.length - 1;
        dst.writeIntLE(count);
        for (int i = 0; i < count; i++) {
            loop[i].toBytes(dst);
        }
    }
}
