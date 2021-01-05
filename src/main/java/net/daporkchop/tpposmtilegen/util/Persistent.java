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

package net.daporkchop.tpposmtilegen.util;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
public interface Persistent {
    static void writeTags(@NonNull ByteBuf dst, @NonNull Map<String, String> tags) {
        if (tags.isEmpty()) {
            dst.writeInt(0);
        } else {
            dst.writeInt(tags.size());
            tags.forEach((k, v) -> {
                int startIndex = dst.writerIndex();
                int bytes = dst.writeInt(-1).writeCharSequence(k, StandardCharsets.UTF_8);
                dst.setInt(startIndex, bytes);

                startIndex = dst.writerIndex();
                bytes = dst.writeInt(-1).writeCharSequence(v, StandardCharsets.UTF_8);
                dst.setInt(startIndex, bytes);
            });
        }
    }

    static Map<String, String> readTags(@NonNull ByteBuf src) {
        int count = src.readInt();
        if (count == 0) {
            return Collections.emptyMap();
        } else {
            Map<String, String> tags = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String k = src.readCharSequence(src.readInt(), StandardCharsets.UTF_8).toString();
                String v = src.readCharSequence(src.readInt(), StandardCharsets.UTF_8).toString();
                tags.put(k, v);
            }
            return tags;
        }
    }

    void toBytes(@NonNull ByteBuf dst);
}
