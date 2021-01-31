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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import lombok.NonNull;
import net.daporkchop.lib.network.nettycommon.PorkNettyHelper;
import net.daporkchop.lib.network.nettycommon.transport.Transport;
import net.daporkchop.tpposmtilegen.http.handle.HttpHandler;

import java.net.InetSocketAddress;

/**
 * @author DaPorkchop_
 */
public class HttpServer implements AutoCloseable {
    protected final Channel channel;

    protected final HttpChannelHandler netHandler = new HttpChannelHandler(this);
    protected final HttpHandler handler;

    public HttpServer(@NonNull InetSocketAddress address, @NonNull HttpHandler handler) {
        this.handler = handler;

        Transport transport = PorkNettyHelper.getTransportTCP();
        EventLoopGroup group = transport.eventLoopGroupPool().get();

        ChannelFuture future = new ServerBootstrap()
                .channelFactory(transport.channelFactorySocketServer())
                .group(group)
                .childHandler(new HttpInitializer(this))
                .bind(address).awaitUninterruptibly();

        if (future.isSuccess()) {
            this.channel = future.channel();
            this.channel.closeFuture().addListener(f -> transport.eventLoopGroupPool().release(group));
        } else {
            transport.eventLoopGroupPool().release(group); //ensure event loop doesn't leak
            throw new RuntimeException(future.cause());
        }
    }

    @Override
    public void close() {
        this.channel.close();
    }
}
