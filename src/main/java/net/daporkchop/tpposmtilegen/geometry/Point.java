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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

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
public final class Point implements Geometry {
    private static final Ref<Matcher> PARSE_MATCHER_CACHE = ThreadRef.regex(Pattern.compile("^(-)?(\\d+)(?:\\.(\\d+))?$"));

    public static final int PRECISION = 10_000_000;
    public static final int UNDEFINED_COORDINATE = 2147483647;

    public static int doubleToFix(double v) {
        return floorI(v * PRECISION);
    }

    public static double fixToDouble(int v) {
        return v * (1.0d / PRECISION);
    }

    public static void appendCoordinate(int c, StringBuilder dst) {
        if (c < 0) {
            dst.append('-');
            c = -c;
        }

        int minD = PRECISION * 100;
        int maxD = 1;
        for (int d = PRECISION * 100; d > 0; d /= 10) {
            if ((c / d) % 10 != 0) {
                minD = min(minD, d);
                maxD = max(maxD, d);
            }
        }

        minD = min(minD, PRECISION / 10);
        maxD = max(maxD, PRECISION);

        for (int d = maxD; d >= minD; d /= 10) {
            dst.append((char) ((c / d) % 10 + '0'));
            if (d == PRECISION) {
                dst.append('.');
            }
        }
    }

    public static int parse(String s) { //my god this is ugly and slow, but it works
        Matcher matcher = PARSE_MATCHER_CACHE.get().reset(s);
        checkArg(matcher.find(), s);
        try {
            int significant = Integer.parseInt(matcher.group(2));
            String frac = matcher.group(3);

            if (frac == null) {
                frac = "0";
            } else if (frac.length() != 7) {
                if (frac.length() > 7) {
                    frac = frac.substring(0, 7);
                } else {
                    StringBuilder builder = new StringBuilder(7);
                    builder.append(frac);
                    PStrings.appendMany(builder, '0', 7 - frac.length());
                    frac = builder.toString();
                }
            }

            int fraction = Integer.parseInt(frac);
            int i = significant * PRECISION + fraction;
            return i * (matcher.group(1) != null ? -1 : 1);
        } catch (Exception e) {
            throw new RuntimeException(s, e);
        }
    }

    private int x;
    private int y;

    public Point() {
        this(UNDEFINED_COORDINATE, UNDEFINED_COORDINATE);
    }

    public Point(double lon, double lat) {
        this(doubleToFix(lon), doubleToFix(lat));
    }

    public Point(String lon, String lat) {
        this(parse(lon), parse(lat));
    }

    public Point(@NonNull ByteBuf src) {
        this(src.readInt(), src.readInt());
    }

    @Override
    public long[] listIntersectedTiles(int level) {
        return new long[]{ point2tile(level, this.x, this.y) };
    }

    @Override
    public void toGeoJSON(@NonNull StringBuilder dst) {
        dst.append("{\"type\":\"Point\",\"coordinates\":[");
        appendCoordinate(this.x, dst);
        dst.append(',');
        appendCoordinate(this.y, dst);
        dst.append(']').append('}');
    }

    @Override
    public Geometry simplify(double targetPointDensity) {
        return null; //we don't allow points to be simplified, they're only ever present at level 0
    }

    @Override
    public WeightedDouble averagePointDensity() {
        return new WeightedDouble(0.0d, 0.0d);
    }

    @Override
    public void toBytes(@NonNull ByteBuf dst) {
        dst.writeInt(this.x).writeInt(this.y);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        appendCoordinate(this.x, builder);
        builder.append(',').append(' ');
        appendCoordinate(this.y, builder);
        builder.append(')');
        return builder.toString();
    }
}
