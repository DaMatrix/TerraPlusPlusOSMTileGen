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

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import net.daporkchop.lib.common.misc.string.PStrings;

import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * Fixed-point geographic coordinate.
 * <p>
 * Adapted from <a href="https://github.com/osmcode/libosmium/blob/392f31c/include/osmium/osm/location.hpp">libosmium/osm/location.hpp</a>.
 *
 * @author DaPorkchop_
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public final class Point implements Comparable<Point>, Persistent<Point> {
    private static final int PRECISION = 10_000_000;
    private static final int UNDEFINED_COORDINATE = 2147483647;

    public static int doubleToFix(double v) {
        return floorI(v * PRECISION);
    }

    public static double fixToDouble(int v) {
        return v * (1.0d / PRECISION);
    }

    private int x;
    private int y;

    public Point() {
        this(UNDEFINED_COORDINATE, UNDEFINED_COORDINATE);
    }

    public Point(double lon, double lat) {
        this(doubleToFix(lon), doubleToFix(lat));
    }

    public Point(@NonNull ByteBuf src) {
        this(src.readInt(), src.readInt());
    }

    public boolean valid() {
        return this.x >= -180 * PRECISION && this.x <= 180 * PRECISION && this.y >= -90 * PRECISION && this.y <= 90 * PRECISION;
    }

    public boolean isDefined() {
        return this.x != UNDEFINED_COORDINATE || this.y != UNDEFINED_COORDINATE;
    }

    public boolean isUndefined() {
        return this.x == UNDEFINED_COORDINATE && this.y == UNDEFINED_COORDINATE;
    }

    public double lon() {
        checkState(this.valid(), "invalid location");
        return fixToDouble(this.x);
    }

    public double lat() {
        checkState(this.valid(), "invalid location");
        return fixToDouble(this.y);
    }

    public Point lon(double lon) {
        return this.x(doubleToFix(lon));
    }

    public Point lat(double lat) {
        return this.y(doubleToFix(lat));
    }

    @Override
    public int compareTo(Point o) {
        if (this.x != o.x) {
            return this.x - o.x;
        } else {
            return this.y - o.y;
        }
    }

    @Override
    public String toString() {
        checkState(this.valid(), "invalid location");
        return PStrings.fastFormat("(%.7f, %.7f)", fixToDouble(this.x), fixToDouble(this.y));
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeInt(this.x).writeInt(this.y);
    }

    @Override
    public Point fromBytes(@NonNull ByteBuf src) {
        return this.x(src.readInt()).y(src.readInt());
    }
}
