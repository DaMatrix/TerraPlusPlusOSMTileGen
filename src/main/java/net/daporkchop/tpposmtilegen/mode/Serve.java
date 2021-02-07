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

package net.daporkchop.tpposmtilegen.mode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.NonNull;
import net.daporkchop.lib.binary.oio.appendable.PAppendable;
import net.daporkchop.lib.binary.oio.appendable.UTF8ByteBufAppendable;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.http.HttpServer;
import net.daporkchop.tpposmtilegen.http.Response;
import net.daporkchop.tpposmtilegen.http.exception.HttpException;
import net.daporkchop.tpposmtilegen.http.HttpHandler;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class Serve implements IMode {
    @Override
    public String name() {
        return "serve";
    }

    @Override
    public String synopsis() {
        return "<index_dir> <port>";
    }

    @Override
    public String help() {
        return "Launches a web server which serves the tiles locally.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: serve <index_dir> <port>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        try (Storage storage = new Storage(src.toPath(), Database.DB_OPTIONS_LITE, true);
             Server server = new Server(Integer.parseUnsignedInt(args[1]), storage, storage.db().read())) {
            new Scanner(System.in).nextLine();
        }
    }

    static class Server implements HttpHandler, AutoCloseable {
        protected final Storage storage;
        protected final DBAccess access;
        protected final HttpServer server;

        public Server(int port, @NonNull Storage storage, @NonNull DBAccess access) {
            this.storage = storage;
            this.access = access;

            this.server = new HttpServer(new InetSocketAddress(port), this);
            logger.success("Server started on port %d", port);
        }

        @Override
        public void handleRequest(@NonNull FullHttpRequest request, @NonNull Response response) throws Exception {
            if (request.method() != HttpMethod.GET) {
                throw new HttpException(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            String path = request.uri().substring(1); //trim leading slash
            if (path.isEmpty()) {
                path = "/";
            }

            ByteBuffer buffer = this.storage.files().get(this.access, path);
            if (buffer != null) {
                response.contentType(HttpHeaderValues.APPLICATION_JSON)
                        .status(HttpResponseStatus.OK)
                        .body(Unpooled.wrappedBuffer(buffer));
            } else {
                List<String> children = this.storage.files().listChildren(this.access, path);
                if (children == null) {
                    throw new HttpException(HttpResponseStatus.NOT_FOUND);
                }
                ByteBuf body = UnpooledByteBufAllocator.DEFAULT.ioBuffer();
                response.contentType("text/html")
                        .status(HttpResponseStatus.OK)
                        .body(body);
                try (PAppendable out = new UTF8ByteBufAppendable(body)) {
                    out.append("<html><body><ul>");
                    children.forEach((IOConsumer<String>) s -> out.appendFmt("<li><a href=\"%1$s\">%1$s</a></li>", s));
                    out.appendLn("</ul></body></html>");
                }
            }
        }

        @Override
        public void close() {
            logger.info("Shutting down...");
            this.server.close();
        }
    }
}
