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

package net.daporkchop.tpposmtilegen.mode.countstrings;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.tpposmtilegen.pipeline.FilterPipelineStep;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.util.NotSynchronizedByteArrayOutputStream;
import net.daporkchop.tpposmtilegen.util.Util;

import java.io.IOException;

/**
 * @author DaPorkchop_
 */
public class ExtractTagStrings extends FilterPipelineStep<ByteBuf, byte[]> {
    protected static final Ref<NotSynchronizedByteArrayOutputStream> BAOS_CACHE = ThreadRef.soft(NotSynchronizedByteArrayOutputStream::new);

    public ExtractTagStrings(PipelineStep<byte[]> next) {
        super(next);
    }

    @Override
    public void accept(@NonNull ByteBuf input) throws IOException {
        //this assumes that the json is a well-formed GeoJSON object (with no additional fields), and not a collection.
        try {
            NotSynchronizedByteArrayOutputStream baos = BAOS_CACHE.get();

            for (int i = input.readerIndex() + 3, limit = input.writerIndex(); i < limit; i++) {
                if (input.getByte(i) == '{' && input.getByte(i - 3) == 's') { //found properties block
                    while (true) {
                        switch (input.getByte(i++)) {
                            case '"': //start string
                                baos.reset();
                                i = Util.readJsonStringToEnd(i, baos, input);
                                this.next.accept(baos.toByteArray());
                                break;
                            case '}': //end object
                                return;
                        }
                    }
                }
            }
        } finally {
            input.release();
        }
    }
}
