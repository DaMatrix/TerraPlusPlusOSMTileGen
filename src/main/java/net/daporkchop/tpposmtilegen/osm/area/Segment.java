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

package net.daporkchop.tpposmtilegen.osm.area;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.util.Point;

import java.util.Arrays;

import static java.lang.Math.*;

/**
 * Adapted from <a href="https://github.com/osmcode/libosmium/blob/392f31c/include/osmium/area/detail/node_ref_segment.hpp>libosmium/area/detail/node_ref_segment.hpp</a>.
 *
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
@Getter
@Setter
final class Segment implements Comparable<Segment> {
    public static boolean outsideXRange(Segment s1, Segment s2) {
        return s1.first.x() > s2.second.x();
    }
    
    public static boolean yRangeOverlap(Segment s1, Segment s2) {
        int minY1 = min(s1.first.y(), s1.second.y());
        int maxY1 = max(s1.first.y(), s1.second.y());
        int minY2 = min(s2.first.y(), s2.second.y());
        int maxY2 = max(s2.first.y(), s2.second.y());
        return !(minY1 > maxY2 || minY2 > maxY1);
    }

    public static boolean intersects(Segment s1, Segment s2) {
        Point p0 = s1.first;
        Point p1 = s1.second;
        Point q0 = s2.first;
        Point q1 = s2.second;
        long p0x = p0.x();
        long p0y = p0.y();
        long p1x = p1.x();
        long p1y = p1.y();
        long q0x = q0.x();
        long q0y = q0.y();
        long q1x = q1.x();
        long q1y = q1.y();

        if ((p0x == q0x && p0y == q0y && p1x == q1x && p1y == q1y)
            || (p0x == q1x && p0y == q1y && p1x == q0x && p1y == q0y)) { //segments are the same
            return false;
        }

        long pdx = (long) p1.x() - (long) p0.x();
        long pdy = (long) p1.y() - (long) p0.y();
        long d = pdx * (q1y - q0y) - pdy * (q1x - q0x);

        if (d != 0L) { //segments aren't collinear
            if (p0.equals(q0) || p0.equals(q1) || p1.equals(q0) || p1.equals(q1)) { //touching at an end point
                return false;
            }

            long na = (q1x - q0x) * (p0y - q0y) - (q1y - q0y) * (p0x - q0x);
            long nb = (p1x - p0x) * (p0y - q0y) - (p1y - p0y) * (p0x - q0x);
            
            return (d > 0L && na >= 0L && na <= d && nb >= 0L && nb <= d) || (d < 0L && na <= 0L && na >= d && nb <= 0L && nb >= d);
        }

        //segments are collinear

        if (pdx * (q0y - p0y) - pdy * (q0x - p0x) == 0L) { //segments are on the same line
            Point[] points = { s1.first, s1.second, s2.first, s2.second};
            Arrays.sort(points);

            return points[1].equals(points[2]);
        }

        return false;
    }

    @NonNull
    protected final Point first;
    @NonNull
    protected final Point second;
    @NonNull
    protected final Role role;
    @NonNull
    protected final Way way;

    protected ProtoRing ring;

    protected boolean reverse;
    protected boolean directionDone;

    public Point first(boolean reverse) {
        return reverse ? this.second : this.first;
    }

    public Point second(boolean reverse) {
        return reverse ? this.first : this.second;
    }

    public Point start() {
        return this.first(this.reverse);
    }

    public Point stop() {
        return this.second(this.reverse);
    }

    public boolean isDone() {
        return this.ring != null;
    }

    public long det() {
        //compute cross product
        Point a = this.start();
        Point b = this.stop();
        return (long) a.x() * (long) b.y() - (long) a.y() * (long) b.x();
    }

    @Override
    public int compareTo(Segment o) {
        return (o == this || this.equals(o)) ? 0 : (this.lessThan(o) ? -1 : 1);
    }

    public boolean lessThan(Segment o) {
        if (this.first.equals(o.first)) {
            Point p0 = this.first;
            Point p1 = this.second;
            Point q0 = o.first;
            Point q1 = o.second;
            long px = (long) p1.x() - (long) p0.x();
            long py = (long) p1.y() - (long) p0.y();
            long qx = (long) q1.x() - (long) q0.x();
            long qy = (long) q1.y() - (long) q0.y();

            if (px == 0L && qx == 0L) {
                return py < qy;
            }

            long a = py * qx;
            long b = qy * px;
            if (a == b) {
                return px < qx;
            } else {
                return a > b;
            }
        }
        return this.first.compareTo(o.first) < 0;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Segment) {
            Segment segment = (Segment) obj;
            return this.first.equals(segment.first) && this.second.equals(segment.second);
        } else {
            return false;
        }
    }
}
