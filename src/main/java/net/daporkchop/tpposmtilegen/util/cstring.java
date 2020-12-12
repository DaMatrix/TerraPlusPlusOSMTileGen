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

import lombok.experimental.UtilityClass;
import net.daporkchop.lib.unsafe.PUnsafe;

/**
 * {@code #include <cstring>}
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class cstring {
    public long strlen(long addr) {
        long len = 0L;
        while (PUnsafe.getByte(addr + len) != 0) {
            len++;
        }
        return len;
    }

    public int strcmp(long a, long b) {
        for (long i = 0L; ; i++) {
            int ca = PUnsafe.getByte(a + i);
            int cb = PUnsafe.getByte(b + i);
            if (ca == cb) { //both characters are the same
                if ((ca | cb) == 0) { //both characters are NUL
                    return 0;
                }
            } else {
                return ca - cb;
            }
        }
    }
}
