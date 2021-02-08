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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.tpposmtilegen.http.exception.HttpException;

import java.io.PrintWriter;

import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
@ChannelHandler.Sharable
class HttpChannelHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    protected static final Logger logger = Logging.logger.channel("HTTP");

    @NonNull
    protected final HttpServer server;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        Response tempResponse = new Response();
        try {
            this.server.handler.handleRequest(request, tempResponse);
        } catch (HttpException e) {
            tempResponse.status(e.status())
                    .contentType(HttpHeaderValues.TEXT_PLAIN)
                    .body(PorkUtil.fallbackIfNull(e.body(), Unpooled.EMPTY_BUFFER).retain());
        } catch (Exception e) {
            ByteBuf body = ByteBufAllocator.DEFAULT.buffer();
            try (PrintWriter writer = new PrintWriter(new ByteBufOutputStream(body))) {
                e.printStackTrace(writer);
            }
            tempResponse.contentType(HttpHeaderValues.TEXT_PLAIN)
                    .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .body(body);
        }

        logger.info("%s %s [%s %s] %d %d",
                ctx.channel().localAddress(), ctx.channel().remoteAddress(),
                request.method(), request.uri(),
                tempResponse.status().code(), tempResponse.body().readableBytes());

        HttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), tempResponse.status(), tempResponse.body());

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, tempResponse.contentType());
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, tempResponse.body().readableBytes());

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
