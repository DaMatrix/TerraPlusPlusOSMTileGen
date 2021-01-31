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

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @author DaPorkchop_
 */
public class PathBasedRouter implements HttpHandler {
    protected final NavigableMap<String, HttpHandler> routes = new TreeMap<>(Comparator.reverseOrder());

    public PathBasedRouter put(@NonNull String prefix, @NonNull HttpHandler handler) {
        this.routes.put(prefix, handler);
        return this;
    }

    @Override
    public ByteBuf handleRequest(@NonNull FullHttpRequest request) throws Exception {
        String uri = request.uri();
        Map.Entry<String, HttpHandler> entry = this.routes.floorEntry(uri);
        if (entry == null || !uri.startsWith(entry.getKey())) {
            throw new HttpException(HttpResponseStatus.NOT_FOUND);
        }
        request.setUri(uri.substring(entry.getKey().length())); //strip path
        return entry.getValue().handleRequest(request);
    }
}
