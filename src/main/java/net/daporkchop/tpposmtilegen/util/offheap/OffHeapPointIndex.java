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

package net.daporkchop.tpposmtilegen.util.offheap;

import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.NonNull;
import net.daporkchop.lib.common.math.BinMath;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.util.MemoryHelper;
import net.daporkchop.tpposmtilegen.util.mmap.MemoryMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class OffHeapPointIndex implements AutoCloseable {
    protected static final int PAGE_SIZE_MASK;

    static {
        checkState(BinMath.isPow2(PUnsafe.PAGE_SIZE), "page size (%d) isn't a power of 2?!?", PUnsafe.PAGE_SIZE);
        PAGE_SIZE_MASK = PUnsafe.PAGE_SIZE - 1;
    }

    protected final MemoryMap map;
    protected final long addr;
    protected final long size;

    public OffHeapPointIndex(@NonNull Path path, long size) throws IOException {
        this.size = positive(size, "maxId");

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            if (channel.size() != size << 3L) { //reset value to default
                channel.truncate(0L);

                int entryCount = 1 << 20 >> 3;
                ByteBuffer buffer = ByteBuffer.allocateDirect(entryCount << 3);

                for (int i = 0; i < entryCount; i++) {
                    buffer.putInt(Integer.MIN_VALUE).putInt(Integer.MIN_VALUE);
                }
                buffer.flip();

                for (long id = 0; id < size; id += entryCount) {
                    buffer.limit(toInt(Math.min(entryCount, size - id) << 3L));
                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    buffer.rewind();
                }

                long realSize = channel.size();
                checkState(realSize == size << 3L, "size: %d (expected: %d)", realSize, size << 3L);
            }
            this.map = new MemoryMap(channel, FileChannel.MapMode.READ_WRITE, 0L, size * 8L);
        }
        this.addr = this.map.addr();
    }

    public void set(long id, @NonNull Point point) {
        long addr = this.addr(id);
        PUnsafe.putInt(addr, point.x());
        PUnsafe.putInt(addr + 4L, point.y());
    }

    public void delete(long id) {
        long addr = this.addr(id);
        PUnsafe.putInt(addr, Integer.MIN_VALUE);
        PUnsafe.putInt(addr + 4L, Integer.MIN_VALUE);
    }

    public Point get(long id) {
        long addr = this.addr(id);
        int x = PUnsafe.getInt(addr);
        int y = PUnsafe.getInt(addr + 4L);
        return x != Integer.MIN_VALUE && y != Integer.MIN_VALUE ? new Point(x, y) : null;
    }

    public List<Point> multiGet(@NonNull LongList ids) {
        int size = ids.size();
        List<Point> points = new ArrayList<>(size);

        if (size == 0) {
            return points;
        } else if (size == 1) {
            points.add(this.get(ids.getLong(0)));
            return points;
        }

        //check all input ids
        for (int i = 0; i < size; i++) {
            checkIndex(this.size, ids.getLong(i));
        }

        //convert to page indices
        long[] pages = new long[size];
        for (int i = 0; i < size; i++) {
            pages[i] = (this.addr + (ids.getLong(i) << 3L)) & ~PAGE_SIZE_MASK;
        }
        Arrays.sort(pages);

        //prefetch pages in bulk
        long last = pages[0];
        long start = last;
        int batchCount = 0;
        for (int i = 1; i < size; i++) {
            long curr = pages[i];
            if (last + 16L * PUnsafe.PAGE_SIZE < curr) { //too big of a jump
                MemoryHelper.prefetch(start, last - start + PUnsafe.PAGE_SIZE);
                start = curr;
                batchCount++;
            }
            last = curr;
        }
        if (batchCount != 0) { //if there's only one sequence, don't bother prefetching (as syscalls are expensive)
            MemoryHelper.prefetch(start, last - start + PUnsafe.PAGE_SIZE);
        }

        //actually get values
        for (int i = 0; i < size; i++) {
            long addr = this.addr + (ids.getLong(i) << 3L);
            int x = PUnsafe.getInt(addr);
            int y = PUnsafe.getInt(addr + 4L);
            points.add(x != Integer.MIN_VALUE && y != Integer.MIN_VALUE ? new Point(x, y) : null);
        }

        return points;
    }

    protected long addr(long id) {
        checkIndex(this.size, id);
        return this.addr + (id << 3L);
    }

    @Override
    public void close() throws IOException {
        this.map.close();
    }
}
