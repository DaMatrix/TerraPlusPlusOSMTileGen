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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.Point;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author DaPorkchop_
 */
class SegmentList extends ArrayList<Segment> {
    public int extractSegmentsFromWay(Storage storage, Way way) throws Exception {
        int count = way.nodes().length;
        if (count == 0) {
            return 0;
        }
        this.ensureCapacity(this.size() + count - 1);
        return this.extractSegmentsFromWay0(storage, way, Role.OUTER);
    }

    public int extractSegmentsFromWays(Storage storage, Relation relation, Way[] ways) throws Exception {
        int segments = Arrays.stream(ways).mapToInt(way -> {
            int count = way.nodes().length;
            return count == 0 ? 0 : count - 1;
        }).sum();
        this.ensureCapacity(this.size() + segments);

        int invalidLocations = 0;
        LongSet ids = new LongOpenHashSet();
        for (int i = 0; i < ways.length; i++) {
            Way way = ways[i];
            if (ids.add(way.id())) {
                Role role = Role.parse(relation.members()[i].role());
                invalidLocations += this.extractSegmentsFromWay0(storage, way, role);
            }
        }

        return invalidLocations;
    }

    private int extractSegmentsFromWay0(Storage storage, Way way, Role role) throws Exception {
        int invalidLocations = 0;

        long[] nodes = way.nodes();
        int count = nodes.length;

        //get points by their IDs
        List<Point> points = storage.points().getAll(LongArrayList.wrap(nodes, count - 1));
        points.add(points.get(0)); //set last point to first point

        Point previous = null;
        for (int i = 0; i < count; i++) {
            Point point = points.get(i);
            if (point == null || !point.valid()) {
                invalidLocations++;
                continue;
            }
            if (previous != null && !previous.equals(point)) {
                this.add(new Segment(previous, point, role, way));
            }
            previous = point;
        }

        return invalidLocations;
    }

    void eraseDuplicateSegments() {
        while (true) {
            int i = 0;
            int lim = this.size() - 1;
            for (; i < lim; i++) {
                if (this.get(i).equals(this.get(i + 1))) {
                    break;
                }
            }

            if (i == lim) { //nothing was found
                return;
            }

            this.removeRange(i, i + 2);
        }
    }

    boolean findIntersections() {
        if (this.isEmpty()) {
            return false;
        }

        for (int i = 0, count = this.size(); i < count; i++) {
            Segment s1 = this.get(i);
            for (int j = i + 1; j < count; j++) {
                Segment s2 = this.get(j);

                if (Segment.outsideXRange(s1, s2)) {
                    break;
                }

                if (Segment.yRangeOverlap(s1, s2) && Segment.intersects(s1, s2)) {
                    return true;
                }
            }
        }

        return false;
    }
}
