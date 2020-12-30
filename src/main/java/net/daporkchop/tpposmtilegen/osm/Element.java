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

package net.daporkchop.tpposmtilegen.osm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of an OpenStreetMap element stored in the CQEngine database.
 *
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
@Getter
@ToString
public abstract class Element {
    protected static final FastThreadLocal<ByteBuf> ENCODING_BUFFER_CACHE = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            return UnpooledByteBufAllocator.DEFAULT.heapBuffer();
        }
    };

    protected final long id;

    @NonNull
    protected Map<String, String> tags;

    public Element(long id, @NonNull ByteBuf data) {
        this.id = id;
        this.fromBytes(data);
    }

    /**
     * @return the state of this element, encoded as a {@code byte[]}
     */
    public byte[] toByteArray() {
        ByteBuf dst = ENCODING_BUFFER_CACHE.get().clear();
        this.toBytes(dst);
        return Arrays.copyOfRange(dst.array(), dst.arrayOffset() + dst.readerIndex(), dst.readableBytes());
    }

    public void toBytes(@NonNull ByteBuf dst) {
        int countIndex = dst.writerIndex();
        dst.writeInt(-1);
        this.tags.forEach((k, v) -> {
            int startIndex = dst.writerIndex();
            int bytes = dst.writeInt(-1).writeCharSequence(k, StandardCharsets.UTF_8);
            dst.setInt(startIndex, bytes);

            startIndex = dst.writerIndex();
            bytes = dst.writeInt(-1).writeCharSequence(v, StandardCharsets.UTF_8);
            dst.setInt(startIndex, bytes);
        });
        dst.setInt(countIndex, this.tags.size());
    }

    public void fromBytes(@NonNull ByteBuf src) {
        this.tags = new HashMap<>();
        for (int i = 0, count = src.readInt(); i < count; i++) {
            String k = src.readCharSequence(src.readInt(), StandardCharsets.UTF_8).toString();
            String v = src.readCharSequence(src.readInt(), StandardCharsets.UTF_8).toString();
            this.tags.put(k, v);
        }
    }
}
