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

package net.daporkchop.tpposmtilegen.util.offheap;

import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.mmap.alloc.sparse.SequentialSparseAllocator;
import net.daporkchop.tpposmtilegen.util.offheap.lock.OffHeapReadWriteLock;

import java.io.IOException;
import java.nio.file.Path;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class OffHeapBVH extends SequentialSparseAllocator {
    /*
     * struct Bounds {
     *   double minX;
     *   double maxX;
     *   double minZ;
     *   double maxZ;
     * };
     */

    protected static final long BOUNDS_MINX_OFFSET = 0L;
    protected static final long BOUNDS_MAXX_OFFSET = BOUNDS_MINX_OFFSET + 8L;
    protected static final long BOUNDS_MINZ_OFFSET = BOUNDS_MAXX_OFFSET + 8L;
    protected static final long BOUNDS_MAXZ_OFFSET = BOUNDS_MINZ_OFFSET + 8L;
    protected static final long BOUNDS_SIZE = BOUNDS_MAXZ_OFFSET + 8L;

    /*
     * struct Node {
     *   ReadWriteLock lock;
     *   Bounds bounds;
     *   Node* children[4];
     *   int size;
     *   int capacity;
     *   void* values;
     *   boolean split;
     * };
     */

    protected static final long NODE_LOCK_OFFSET = 0L;
    protected static final long NODE_BOUNDS_OFFSET = NODE_LOCK_OFFSET + OffHeapReadWriteLock.SIZE;
    protected static final long NODE_CHILDREN_OFFSET = NODE_BOUNDS_OFFSET + BOUNDS_SIZE;
    protected static final long NODE_SIZE_OFFSET = NODE_CHILDREN_OFFSET + 4 * 8L;
    protected static final long NODE_CAPACITY_OFFSET = NODE_SIZE_OFFSET + 4L;
    protected static final long NODE_VALUES_OFFSET = NODE_SIZE_OFFSET + 4L;
    protected static final long NODE_SPLIT_OFFSET = NODE_SIZE_OFFSET + 8L;
    protected static final long NODE_SIZE = NODE_SPLIT_OFFSET + 1L;

    protected static final int INITIAL_NODE_CAPACITY = 8;
    protected static final int NODE_SPLIT_CAPACITY = 8;

    private final long base = super.headerSize();
    protected final long root;

    protected final Bounds2d bounds;

    public OffHeapBVH(@NonNull Path path, @NonNull Bounds2d bounds, long size) throws IOException {
        super(path, size);

        this.bounds = bounds;

        long addr = this.addr;
        long root = PUnsafe.getLong(addr + this.base);
        if (root == 0L) { //root node doesn't exist, allocate it
            root = this.allocateNode(bounds.minX(), bounds.maxX(), bounds.minZ(), bounds.maxZ());
        }
        this.root = root;
    }

    protected long allocateNode(double minX, double maxX, double minZ, double maxZ) {
        long addr = this.addr;
        long node = this.alloc(NODE_SIZE);

        //clear allocated memory block
        PUnsafe.setMemory(addr + node, NODE_SIZE, (byte) 0);
        OffHeapReadWriteLock.init(node + NODE_LOCK_OFFSET);

        //set bounds
        PUnsafe.putDouble(node + NODE_BOUNDS_OFFSET + BOUNDS_MINX_OFFSET, minX);
        PUnsafe.putDouble(node + NODE_BOUNDS_OFFSET + BOUNDS_MAXX_OFFSET, maxX);
        PUnsafe.putDouble(node + NODE_BOUNDS_OFFSET + BOUNDS_MINZ_OFFSET, minZ);
        PUnsafe.putDouble(node + NODE_BOUNDS_OFFSET + BOUNDS_MAXZ_OFFSET, maxZ);

        //initialize empty values list with default capacity
        PUnsafe.putInt(node + NODE_CAPACITY_OFFSET, INITIAL_NODE_CAPACITY);
        long values = this.alloc(INITIAL_NODE_CAPACITY * 8L);
        PUnsafe.setMemory(values, INITIAL_NODE_CAPACITY * 8L, (byte) 0);
        PUnsafe.putLong(node + NODE_VALUES_OFFSET, values);

        return node;
    }

    public void insert(@NonNull Bounds2d value) {
        checkArg(this.bounds.contains(value), "value %s isn't contained in %s!", value, this.bounds);

        try (Bounds2d bounds = Bounds2d.blank()) {
            long addr = this.addr;
            long node = addr + this.root;
            while (true) {
                if (PUnsafe.getInt(node + NODE_SIZE_OFFSET) == NODE_SPLIT_CAPACITY && PUnsafe.getByte(null, node + NODE_SPLIT_OFFSET) == 0) {
                    //current node is at splitting capacity and hasn't split yet, so attempt to split before continuing
                }
            }
        }
    }

    protected void split(long node) {
        int size = PUnsafe.getInt(node + NODE_SIZE_OFFSET);
        PUnsafe.putInt(node + NODE_SIZE_OFFSET, 0);
        long values = this.addr + PUnsafe.getLong(node + NODE_VALUES_OFFSET);

        long[] oldValues = new long[size]; //copy to temporary array
        PUnsafe.copyMemory(null, values, oldValues, PUnsafe.ARRAY_LONG_BASE_OFFSET, size * 8L);
    }

    protected int childIndex(Bounds2d nodeBounds, Bounds2d bounds) { //we assume the node contains the given bounds
        double mx = (nodeBounds.minX() + nodeBounds.maxX()) * 0.5d;
        double mz = (nodeBounds.minZ() + nodeBounds.maxZ()) * 0.5d;

        int i = 0;
        if (bounds.maxX() <= mx) {
            i |= 0; //lmao this does nothing
        } else if (bounds.minX() >= mx) {
            i |= 1;
        } else {
            i |= -1;
        }

        if (bounds.maxZ() <= mz) {
            i |= 0;
        } else if (bounds.minZ() >= mz) {
            i |= 2;
        } else {
            i |= -1;
        }
        return i;
    }

    @Override
    protected long headerSize() {
        return super.headerSize() + 8L;
    }

    @Override
    protected void initHeaders() {
        super.initHeaders();
    }

    @Override
    public void close() throws IOException {
        super.close();

        this.bounds.close();
    }
}
