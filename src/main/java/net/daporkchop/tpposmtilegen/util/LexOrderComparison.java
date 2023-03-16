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

package net.daporkchop.tpposmtilegen.util;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.annotation.param.NotNegative;
import net.daporkchop.tpposmtilegen.natives.Memory;

import java.util.Comparator;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class LexOrderComparison {
    public static Comparator<byte[]> heapComparator() {
        return LexOrderComparison::compare;
    }

    public static int compare(byte[] arr1, byte[] arr2) {
        for (int i = 0; i < arr1.length && i < arr2.length; i++) {
            int diff = (arr1[i] & 0xFF) - (arr2[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return arr1.length - arr2.length; //Integer.compare(arr1.length, arr2.length);
    }

    public static int compare(@NonNull byte[] arr1, long arr2, @NotNegative long length2) {
        int diff = Memory.memcmp(arr1, 0, arr2, (int) Math.min(arr1.length, length2));
        return diff != 0 ? diff : (int) (arr1.length - length2);
    }

    public static int compare(long arr1, @NotNegative long length1, @NonNull byte[] arr2) {
        int diff = Memory.memcmp(arr1, arr2, 0, (int) Math.min(length1, arr2.length));
        return diff != 0 ? diff : (int) (length1 - arr2.length);
    }

    public static int compare(long arr1, @NotNegative long length1, long arr2, @NotNegative long length2) {
        int diff = Memory.memcmp(arr1, arr2, Math.min(length1, length2));
        return diff != 0 ? diff : (int) (length1 - length2);
    }
}
