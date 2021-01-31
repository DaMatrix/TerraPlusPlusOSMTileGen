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

package net.daporkchop.tpposmtilegen.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.tpposmtilegen.http.exception.HttpException;

import java.io.PrintWriter;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
@ChannelHandler.Sharable
class HttpChannelHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    @NonNull
    protected final HttpServer server;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        ByteBuf body;
        AsciiString contentType = HttpHeaderValues.APPLICATION_JSON;
        HttpResponseStatus status = HttpResponseStatus.OK;
        try {
            body = this.server.handler.handleRequest(request);
        } catch (HttpException e) {
            if ((body = e.body()) != null) {
                body.retain();
            }
            status = e.status();
        } catch (Exception e) {
            contentType = HttpHeaderValues.TEXT_PLAIN;
            status = HttpResponseStatus.BAD_REQUEST;
            body = ByteBufAllocator.DEFAULT.buffer();
            try (PrintWriter writer = new PrintWriter(new ByteBufOutputStream(body))) {
                e.printStackTrace(writer);
            }
        }

        HttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), status, body != null ? body : Unpooled.EMPTY_BUFFER);

        if (body != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes());
        } else {
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        }

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response, ctx.voidPromise());
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
