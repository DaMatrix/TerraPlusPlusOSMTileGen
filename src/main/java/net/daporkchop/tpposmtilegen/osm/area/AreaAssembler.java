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

import it.unimi.dsi.fastutil.ints.IntArrays;
import net.daporkchop.tpposmtilegen.util.Point;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author DaPorkchop_
 */
public class AreaAssembler {
    protected final SegmentList segmentList = new SegmentList();
    protected final ArrayList<Point> splitLocations = new ArrayList<>();
    protected int[] locations;
    protected int numMembers = 0;

    boolean createRings() {
        this.segmentList.sort(Comparator.naturalOrder());
        this.segmentList.eraseDuplicateSegments();

        if (this.segmentList.isEmpty()) {
            return false;
        }

        if (this.waysWereLost()) {
            return false;
        }

        if (this.segmentList.findIntersections()) {
            return false;
        }

        int[] locations = this.createLocationsList();

        if (!this.findSplitLocations(locations)) {
            return false;
        }

        if (this.splitLocations.isEmpty()) {

        }
    }

    boolean waysWereLost() {
        return this.segmentList.stream().mapToLong(s -> s.way.id()).distinct().count() < this.numMembers;
    }

    int[] createLocationsList() {
        int count = this.segmentList.size() << 1;
        int[] locations = new int[count];
        for (int i = 0; i < count; i++) {
            locations[i] = i;
        }

        IntArrays.stableSort(locations, (a, b) ->
                this.segmentList.get(a >>> 1).first((a & 1) != 0).compareTo(this.segmentList.get(b >>> 1).first((b & 1) != 0)));

        return locations;
    }

    boolean findSplitLocations(int[] locations) {
        Point previous = null;
        for (int i = 0; i < locations.length; i++) {
            int iloc = locations[i];
            Point loc = this.segmentList.get(iloc >>> 1).first((iloc & 1) != 0);
            if (i + 1 == locations.length || !loc.equals(this.segmentList.get((iloc + 1) >>> 1).first((iloc & 1) == 0))) {
                return false;
            }
            if (loc.equals(previous) && (this.splitLocations.isEmpty() || !loc.equals(this.splitLocations.get(this.splitLocations.size() - 1)))) {
                this.splitLocations.add(previous);
            }
            if (++i == locations.length) {
                break;
            }
            previous = loc;
        }
        return true;
    }

    void createRingsSimpleCase(int[] locations) {
        int countRemaining = this.segmentList.size();
        for (int loc : locations) {
            Segment segment = this.segmentList.get(loc >>> 1);
            if (!segment.isDone()) {
                countRemaining -= this.addNewRing(loc, segment);
            }
        }
    }

    int addNewRing(int node, Segment segment) {
        if ((node & 1) != 0) {
            segment.reverse(!segment.reverse());
        }

        ProtoRing outerRing = null;
        if (segment != this.segmentList.get(0)) {
            
        }
    }
}
