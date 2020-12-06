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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Alternative to {@link ByteArrayOutputStream} without synchronization.
 *
 * @author DaPorkchop_
 */
public class NotSynchronizedByteArrayOutputStream extends OutputStream {
    protected byte[] b = new byte[512];
    @Getter
    protected int size = 0;

    @Override
    public void write(int b) throws IOException {
        if (this.size + 1 == this.b.length) { //grow array
            byte[] newArr = new byte[this.b.length << 1];
            System.arraycopy(this.b, 0, newArr, 0, this.b.length);
            this.b = newArr;
        }
        this.b[this.size++] = (byte) b;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(this.b, this.size);
    }

    public void reset() {
        this.size = 0;
    }
}

