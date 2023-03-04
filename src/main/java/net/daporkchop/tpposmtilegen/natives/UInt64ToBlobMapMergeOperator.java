/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2023 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.natives;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import lombok.NonNull;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import org.rocksdb.MergeOperator;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class UInt64ToBlobMapMergeOperator extends MergeOperator {
    static {
        PUnsafe.ensureClassInitialized(Natives.class);
        init();
    }

    public static final UInt64ToBlobMapMergeOperator INSTANCE = new UInt64ToBlobMapMergeOperator();

    private static native void init();

    private static native long create();

    public UInt64ToBlobMapMergeOperator() {
        super(create());
    }

    @Override
    protected native void disposeInternal(long handle);

    public static void add(@NonNull ByteBuf buf, long key, @NonNull byte[] data) {
        buf.ensureWritable(12 + data.length)
                .writeLongLE(key)
                .writeIntLE(data.length)
                .writeBytes(data);
    }

    public static void add(@NonNull ByteBuf buf, long key, @NonNull ByteBuf data) {
        buf.ensureWritable(12 + data.readableBytes())
                .writeLongLE(key)
                .writeIntLE(data.readableBytes())
                .writeBytes(data);
    }

    public static byte[] add(long key, @NonNull byte[] data) {
        byte[] arr = new byte[12 + data.length];
        add(Unpooled.wrappedBuffer(arr).clear(), key, data);
        return arr;
    }

    public static byte[] add(long key, @NonNull String data) {
        return add(key, data.getBytes(StandardCharsets.UTF_8));
    }

    public static void del(@NonNull ByteBuf buf, long key) {
        buf.ensureWritable(12).writeLongLE(key).writeIntLE(-1);
    }

    public static byte[] del(long key) {
        byte[] arr = new byte[12];
        del(Unpooled.wrappedBuffer(arr).clear(), key);
        return arr;
    }

    public static void decodeToSlices(@NonNull ByteBuf buf, @NonNull LongObjConsumer<? super ByteBuf> action) {
        while (buf.isReadable()) {
            long key = buf.readLongLE();
            int size = buf.readIntLE();
            checkState(size >= 0, "value contains a delete for key %d?!?", key);
            action.accept(key, buf.readSlice(size));
        }
    }

    public static void decodeToArrays(@NonNull ByteBuf buf, @NonNull LongObjConsumer<? super byte[]> action) {
        decodeToSlices(buf, (key, slice) -> {
            byte[] arr = new byte[slice.readableBytes()];
            slice.readBytes(arr);
            action.accept(key, arr);
        });
    }

    public static void decodeToStrings(@NonNull ByteBuf buf, @NonNull LongObjConsumer<? super String> action) {
        decodeToSlices(buf, (key, slice) -> action.accept(key, slice.toString(StandardCharsets.UTF_8)));
    }

    public static Long2ObjectSortedMap<String> decodeToStrings(@NonNull ByteBuf buf) {
        Long2ObjectSortedMap<String> map = new Long2ObjectRBTreeMap<>();
        decodeToStrings(buf, map::put);
        return map;
    }

    public static Long2ObjectSortedMap<String> decodeToStrings(@NonNull byte[] arr) {
        return decodeToStrings(Unpooled.wrappedBuffer(arr));
    }
}
