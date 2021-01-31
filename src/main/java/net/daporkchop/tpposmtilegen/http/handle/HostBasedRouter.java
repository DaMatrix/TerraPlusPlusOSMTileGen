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

package net.daporkchop.tpposmtilegen.http.handle;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.http.exception.HttpException;

import java.util.HashMap;
import java.util.Map;

/**
 * Routes requests to delegate handlers based on the {@code Host} header.
 *
 * @author DaPorkchop_
 */
public class HostBasedRouter implements HttpHandler {
    protected final Map<String, HttpHandler> delegates = new HashMap<>();

    public HostBasedRouter put(@NonNull String host, @NonNull HttpHandler handler) {
        this.delegates.put(host, handler);
        return this;
    }

    @Override
    public ByteBuf handleRequest(@NonNull FullHttpRequest request) throws Exception {
        String host = request.headers().get("Host");
        HttpHandler handler = this.delegates.get(host);
        if (handler == null) {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST);
        }
        return handler.handleRequest(request);
    }
}
