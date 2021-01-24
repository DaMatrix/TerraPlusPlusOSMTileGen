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

import lombok.experimental.UtilityClass;
import net.daporkchop.lib.binary.oio.StreamUtil;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.unsafe.PUnsafe;

import java.io.InputStream;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class MemoryHelper {
    private final MappedByteBuffer MAPPED_BUFFER = PUnsafe.allocateInstance(PorkUtil.classForName("java.nio.DirectByteBuffer"));

    private final Lambda load0;

    static {
        try {
            //load Lambda class from system classloader
            byte[] lambdaClass;
            try (InputStream in = MemoryHelper.class.getResourceAsStream("/net/daporkchop/tpposmtilegen/util/MemoryHelper$Lambda.class")) {
                lambdaClass = StreamUtil.toByteArray(in);
            }
            PUnsafe.defineClass("net/daporkchop/tpposmtilegen/util/MemoryHelper$Lambda", lambdaClass, 0, lambdaClass.length,
                    MappedByteBuffer.class.getClassLoader(), null);

            PUnsafe.ensureClassInitialized(Lambda.class);

            MethodHandles.Lookup original = MethodHandles.lookup();
            Field internal = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");
            internal.setAccessible(true);
            MethodHandles.Lookup trusted = (MethodHandles.Lookup) internal.get(original);

            MethodHandles.Lookup caller = trusted.in(MappedByteBuffer.class);

            Method method = MappedByteBuffer.class.getDeclaredMethod("load0", long.class, long.class);
            method.setAccessible(true);
            MethodHandle handle = caller.unreflect(method);

            final Class<?> functionClass = Lambda.class;
            final String functionName = "accept";
            final Class<?> functionReturn = void.class;
            final Class<?>[] functionParams = { MappedByteBuffer.class, long.class, long.class };

            MethodType factoryMethodType = MethodType.methodType(functionClass);
            MethodType functionMethodType = MethodType.methodType(functionReturn, functionParams);

            CallSite setterFactory = LambdaMetafactory.metafactory(
                    caller,
                    functionName,
                    factoryMethodType,
                    functionMethodType,
                    handle,
                    handle.type());

            load0 = (Lambda) setterFactory.getTarget().invokeExact();
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

    public void prefetch(long addr, long length) {
        load0.accept(MAPPED_BUFFER, addr, positive(length, "length"));
    }

    public interface Lambda {
        void accept(MappedByteBuffer buffer, long addr, long length);
    }
}
