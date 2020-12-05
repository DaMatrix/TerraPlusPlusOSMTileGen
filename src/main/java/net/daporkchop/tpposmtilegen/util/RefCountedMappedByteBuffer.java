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

package net.daporkchop.tpposmtilegen.util;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.refcount.AbstractRefCounted;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.lib.unsafe.util.exception.AlreadyReleasedException;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
public final class RefCountedMappedByteBuffer extends AbstractRefCounted {
    protected final long addr;
    protected final long size;

    public RefCountedMappedByteBuffer(@NonNull FileChannel channel, @NonNull FileChannel.MapMode mode, long position, long size) {
        checkArg(channel.isOpen(), "FileChannel is closed!");
        notNegative(position, "position");
        this.size = notNegative(size, "size");

        try {
            Class<?> clazz = Class.forName("sun.nio.ch.FileChannelImpl");

            String smode;
            if (mode == FileChannel.MapMode.READ_ONLY) {
                smode = "MAP_RO";
            } else if (mode == FileChannel.MapMode.READ_WRITE) {
                smode = "MAP_RW";
            } else if (mode == FileChannel.MapMode.PRIVATE) {
                smode = "MAP_PV";
            } else {
                throw new IllegalArgumentException(mode.toString());
            }
            int imode = PUnsafe.pork_getStaticField(clazz, smode).getInt();

            Method m = clazz.getDeclaredMethod("map0", int.class, long.class, long.class);
            m.setAccessible(true);
            this.addr = (long) m.invoke(channel, imode, position, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RefCountedMappedByteBuffer retain() throws AlreadyReleasedException {
        super.retain();
        return this;
    }

    @Override
    protected void doRelease() {
        System.out.println("Released memory map");
        try {
            Class<?> clazz = Class.forName("sun.nio.ch.FileChannelImpl");

            Method m = clazz.getDeclaredMethod("unmap0", long.class, long.class);
            m.setAccessible(true);
            m.invoke(null, this.addr, this.size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
