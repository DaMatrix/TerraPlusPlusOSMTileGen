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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.SimpleRecycler;
import net.daporkchop.tpposmtilegen.util.Utils;

import java.awt.Polygon;
import java.util.NavigableSet;
import java.util.TreeSet;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
public abstract class ComplexGeometry implements Geometry {
    protected static final Ref<PolygonRecycler> POLYGON_RECYCLER = ThreadRef.soft(PolygonRecycler::new);

    protected static Point[] simplifyVisvalingamWhyatt(@NonNull Point[] inPoints, int targetLevel, boolean closed) {
        double minimumDensity = Utils.minimumDensityAtLevel(targetLevel);

        @RequiredArgsConstructor
        class Node implements Comparable<Node> {
            @NonNull
            final Point point;
            final int idx;

            Node prev;
            Node next;

            public boolean valid() {
                return this.prev != null && this.next != null;
            }

            @Override
            public int compareTo(Node o) {
                checkState(this.valid());

                int d = Double.compare(this.area(), o.area());
                if (d == 0) {
                    d = Integer.compare(this.idx, o.idx);
                }
                return d;
            }

            public double area() {
                Point p0 = this.prev.point;
                Point p1 = this.point;
                Point p2 = this.next.point;

                double x0 = p0.x();
                double y0 = p0.y();
                double x1 = p1.x();
                double y1 = p1.y();
                double x2 = p2.x();
                double y2 = p2.y();

                return 0.5d * abs(x0 * y1 + x1 * y2 + x2 * y0 - x0 * y2 - x1 * y0 - x2 * y1);
            }

            @Override
            public String toString() {
                return "node#" + this.idx + '@' + this.point + (this.valid() ? " area=" + this.area() : " (invalid)");
            }
        }

        NavigableSet<Node> nodeSet = new TreeSet<>();
        Node head, tail;

        {
            Node[] nodes = new Node[inPoints.length];
            for (int i = 0; i < inPoints.length; i++) {
                nodes[i] = new Node(inPoints[i], i);
            }
            head = nodes[0];
            tail = nodes[nodes.length - 1];

            head.next = nodes[1];
            tail.prev = nodes[nodes.length - 2];

            for (int i = 1; i < nodes.length - 1; i++) {
                nodes[i].prev = nodes[i - 1];
                nodes[i].next = nodes[i + 1];
            }

            for (int i = 1; i < nodes.length - 1; i++) {
                nodeSet.add(nodes[i]);
            }
        }

        double totalLength = 0.0d;
        double totalSegments = 0.0d;
        for (int i = 1; i < inPoints.length; i++, totalSegments++) {
            totalLength += inPoints[i - 1].distance(inPoints[i]);
        }

        int stopSize = nodeSet.size() >> targetLevel;
        while (!nodeSet.isEmpty() && nodeSet.size() >= stopSize && totalLength / totalSegments < minimumDensity) {
            Node node = nodeSet.pollFirst();

            if (node.prev != head) {
                checkState(nodeSet.remove(node.prev));
            }
            if (node.next != tail) {
                checkState(nodeSet.remove(node.next));
            }

            totalLength -= node.point.distance(node.prev.point);
            totalLength -= node.point.distance(node.next.point);
            totalLength += node.prev.point.distance(node.next.point);
            totalSegments--;

            node.prev.next = node.next;
            node.next.prev = node.prev;

            if (node.prev != head) {
                checkState(nodeSet.add(node.prev));
            }
            if (node.next != tail) {
                checkState(nodeSet.add(node.next));
            }

            node.prev = node.next = null;
        }

        int outPointCount = nodeSet.size() + 2; //# of points remaining in set, plus 2 for head+tail

        //determine whether or not to discard the line
        if (closed) {
            if (outPointCount < 4) { //not enough points remaining for a valid closed loop
                return null;
            }
        } else {
            if (outPointCount == 2) { //only 2 points remain (head and tail nodes, respectively)
                double dist = head.point.distance(tail.point);
                if (dist < minimumDensity) { //what remains of the line is too short, discard it
                    return null;
                }
            }
        }

        Point[] outPoints = new Point[outPointCount];
        int i = 0;
        for (Node node = head; node != null; node = node.next) {
            outPoints[i++] = node.point;
        }

        return outPoints;
    }

    @Override
    public final long[] listIntersectedTiles(int level) {
        Bounds2d bounds = this.bounds();
        int tileMinX = point2tile(level, bounds.minX());
        int tileMaxX = point2tile(level, bounds.maxX());
        int tileMinY = point2tile(level, bounds.minY());
        int tileMaxY = point2tile(level, bounds.maxY());
        int tileCount = (tileMaxX - tileMinX + 1) * (tileMaxY - tileMinY + 1);

        if (tileCount == 0) { //wtf?!?
            throw new IllegalStateException();
        } else if (tileCount == 1) { //only intersects a single tile
            return new long[]{ xy2tilePos(tileMinX, tileMinY) };
        } else if (tileMinX == tileMaxX || tileMinY == tileMaxY) { //only intersects a single row/column of tiles
            long[] arr = new long[tileCount];
            for (int i = 0, x = tileMinX; x <= tileMaxX; x++) {
                for (int y = tileMinY; y <= tileMaxY; y++) {
                    arr[i++] = xy2tilePos(x, y);
                }
            }
            return arr;
        } else {
            return this.listIntersectedTilesComplex(level, tileMinX, tileMaxX, tileMinY, tileMaxY, tileCount);
        }
    }

    protected long[] listIntersectedTilesComplex(int level, int tileMinX, int tileMaxX, int tileMinY, int tileMaxY, int tileCount) {
        long[] arr = new long[tileCount];
        for (int i = 0, x = tileMinX; x <= tileMaxX; x++) {
            for (int y = tileMinY; y <= tileMaxY; y++) {
                arr[i++] = xy2tilePos(x, y);
            }
        }
        return arr;
    }

    protected static final class PolygonRecycler extends SimpleRecycler<Polygon> {
        public Polygon fromClosedLineString(@NonNull Point[] points) {
            Polygon polygon = this.get();

            for (int i = 0, lim = points.length - 1; i < lim; i++) { //skip last point because polygon is closed
                polygon.addPoint(points[i].x(), points[i].y());
            }
            return polygon;
        }

        @Override
        protected Polygon newInstance0() {
            return new Polygon();
        }

        @Override
        protected void reset0(@NonNull Polygon value) {
            value.reset();
        }

        @Override
        protected boolean hasCapacity() {
            return this.size() < 8; //don't store more than 8 polygon instances
        }
    }
}
