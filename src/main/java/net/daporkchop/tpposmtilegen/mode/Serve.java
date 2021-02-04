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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.http.HttpServer;
import net.daporkchop.tpposmtilegen.http.exception.HttpException;
import net.daporkchop.tpposmtilegen.http.handle.HttpHandler;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import org.rocksdb.DBOptions;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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

        try (DBOptions options = new DBOptions(Database.DB_OPTIONS)
                .setMaxOpenFiles(64)
                .setMaxFileOpeningThreads(1);
             Storage storage = new Storage(src.toPath(), options);
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
        public ByteBuf handleRequest(@NonNull FullHttpRequest request) throws Exception {
            if (request.method() != HttpMethod.GET) {
                throw new HttpException(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            ByteBuffer buffer = this.storage.files().get(this.access, request.uri().substring(1));
            if (buffer == null) {
                throw new HttpException(HttpResponseStatus.NOT_FOUND);
            }

            return Unpooled.wrappedBuffer(buffer);
        }

        @Override
        public void close() {
            logger.info("Shutting down...");
            this.server.close();
        }
    }
}
