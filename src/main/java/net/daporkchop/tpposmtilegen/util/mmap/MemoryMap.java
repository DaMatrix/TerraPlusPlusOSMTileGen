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

package net.daporkchop.tpposmtilegen.util.mmap;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.unsafe.PCleaner;
import net.daporkchop.lib.unsafe.PUnsafe;

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
public class MemoryMap implements AutoCloseable {
    protected static final Class<?> FILE_CHANNEL_IMPL = PorkUtil.classForName("sun.nio.ch.FileChannelImpl");
    protected static final int IMODE_RO = PUnsafe.pork_getStaticField(FILE_CHANNEL_IMPL, "MAP_RO").getInt();
    protected static final int IMODE_RW = PUnsafe.pork_getStaticField(FILE_CHANNEL_IMPL, "MAP_RW").getInt();
    protected static final int IMODE_PV = PUnsafe.pork_getStaticField(FILE_CHANNEL_IMPL, "MAP_PV").getInt();

    protected static long map0(@NonNull FileChannel channel, int imode, long position, long size) {
        try {
            Method m = FILE_CHANNEL_IMPL.getDeclaredMethod("map0", int.class, long.class, long.class);
            m.setAccessible(true);
            return (long) m.invoke(channel, imode, position, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void unmap0(long addr, long size) {
        try {
            Method m = FILE_CHANNEL_IMPL.getDeclaredMethod("unmap0", long.class, long.class);
            m.setAccessible(true);
            m.invoke(null, addr, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void truncate0(@NonNull FileChannel channel, long size) {
        try {
            Field fdField = FILE_CHANNEL_IMPL.getDeclaredField("fd");
            fdField.setAccessible(true);
            Object fd = fdField.get(channel);

            Field ndField = FILE_CHANNEL_IMPL.getDeclaredField("nd");
            ndField.setAccessible(true);
            Object nd = ndField.get(channel);

            Method truncate = ndField.getType().getDeclaredMethod("truncate", FileDescriptor.class, long.class);
            truncate.setAccessible(true);
            truncate.invoke(nd, fd, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected final long addr;
    protected final long size;

    @Getter(AccessLevel.NONE)
    protected final PCleaner cleaner;

    public MemoryMap(@NonNull FileChannel channel, @NonNull FileChannel.MapMode mode, long position, long size) {
        checkArg(channel.isOpen(), "FileChannel is closed!");
        notNegative(position, "position");
        this.size = notNegative(size, "size");

        if (this.size == 0L) { //don't create zero-length mapping
            this.addr = 0L;
            this.cleaner = null;
            return;
        }

        try {
            int imode;
            if (mode == FileChannel.MapMode.READ_ONLY) {
                imode = IMODE_RO;
            } else if (mode == FileChannel.MapMode.READ_WRITE) {
                imode = IMODE_RW;
            } else if (mode == FileChannel.MapMode.PRIVATE) {
                imode = IMODE_PV;
            } else {
                throw new IllegalArgumentException(mode.toString());
            }

            this.addr = map0(channel, imode, position, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.cleaner = PCleaner.cleaner(this, new Unmapper(this.addr, this.size));
    }

    @Override
    public void close() {
        this.cleaner.clean();
    }

    @RequiredArgsConstructor
    protected static final class Unmapper implements Runnable {
        protected final long addr;
        protected final long size;

        @Override
        public void run() {
            unmap0(this.addr, this.size);
            System.out.println("Released memory map");
        }
    }
}
