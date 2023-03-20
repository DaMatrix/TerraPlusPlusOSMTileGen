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

package net.daporkchop.tpposmtilegen.mode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.NonNull;
import lombok.SneakyThrows;
import net.daporkchop.lib.binary.oio.appendable.PAppendable;
import net.daporkchop.lib.binary.oio.appendable.UTF8ByteBufAppendable;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.http.HttpHandler;
import net.daporkchop.tpposmtilegen.http.HttpServer;
import net.daporkchop.tpposmtilegen.http.Response;
import net.daporkchop.tpposmtilegen.http.exception.HttpException;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.util.Tile;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.tpposmtilegen.util.Utils.*;

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
        return "<index_dir> <port> <lite>";
    }

    @Override
    public String help() {
        return "Launches a web server which serves the tiles locally.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 3, "Usage: serve <index_dir> <port> <lite>");
        Path src = PFiles.assertDirectoryExists(Paths.get(args[0]));
        boolean lite = Boolean.parseBoolean(args[2]);

        try (Storage storage = new Storage(src, lite ? DatabaseConfig.RW_LITE : DatabaseConfig.RW_GENERAL);
             Server server = new Server(Integer.parseUnsignedInt(args[1]), storage, storage.db().read())) {
            new Scanner(System.in).nextLine();
        }
    }

    static class Server implements HttpHandler, AutoCloseable {
        protected final Storage storage;
        protected final DBReadAccess access;
        protected final HttpServer server;
        protected final Path customRoot;

        public Server(int port, @NonNull Storage storage, @NonNull DBReadAccess access) {
            this.storage = storage;
            this.access = access;

            this.server = new HttpServer(new InetSocketAddress(port), this);
            logger.success("Server started on port %d", port);

            this.customRoot = storage.root().resolve("custom");
        }

        @Override
        public void handleRequest(@NonNull FullHttpRequest request, @NonNull Response response) throws Exception {
            if (request.method() != HttpMethod.GET) {
                throw new HttpException(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            String path = request.uri();
            {
                int paramIndex = path.indexOf('?');
                if (paramIndex >= 0) { //trim url parameters
                    path = path.substring(0, paramIndex);
                }

                path = path.substring(1); //trim leading slash
                if (!path.isEmpty() && path.charAt(path.length() - 1) == '/') { //trim trailing slash
                    path = path.substring(0, path.length() - 1);
                }
            }

            if (path.isEmpty()) {
                this.sendIndex(response, path, IntStream.range(0, MAX_LEVEL).mapToObj(i -> i + "/"));
                return;
            }

            String[] split = path.split("/");

            int level = Integer.parseUnsignedInt(split[0]);
            if (split.length == 1) {
                this.sendIndex(response, path, Stream.of("way/", "relation/", "coastline/", "tile/"));
                return;
            }

            switch (split[1]) {
                default:
                    throw new IllegalStateException();
                case "tile": {
                    if (split.length == 2) {
                        this.sendIndex(response, path, IntStream.rangeClosed(Tile.point2tile(level, -180 * Point.PRECISION), Tile.point2tile(level, 180
                                                                                                                                                    * Point.PRECISION))
                                .mapToObj(i -> i + "/"));
                        return;
                    }

                    int tileX = Integer.parseInt(split[2]);
                    if (split.length == 3) {
                        this.sendIndex(response, path, IntStream.rangeClosed(Tile.point2tile(level, -90 * Point.PRECISION), Tile.point2tile(level, 90 * Point.PRECISION))
                                .mapToObj(i -> i + ".json"));
                        return;
                    }

                    checkArg(split.length == 4);
                    checkArg(split[3].endsWith(".json"));
                    int tileY = Integer.parseInt(split[3].substring(0, split[3].length() - ".json".length()));

                    response.status(HttpResponseStatus.OK)
                            .contentType("application/geo+json")
                            .body(this.storage.getTile(this.access, tileX, tileY, level));
                    return;
                }
                case "way":
                case "relation":
                case "coastline": {
                    if (split.length == 2) {
                        this.sendIndex(response, path, IntStream.range(0, 1000).mapToObj(i -> PStrings.fastFormat("%03d/", i)));
                        return;
                    }
                }
            }

            int idPart = Integer.parseUnsignedInt(split[2]);
            checkIndex(1000, idPart);
            long id;
            if ("coastline".equals(split[1])) {
                id = idPart * 1000L;

                if (split.length == 3) {
                    this.sendIndex(response, path, IntStream.range(0, 1000).mapToObj(i -> i + ".json"));
                    return;
                }

                checkArg(split.length == 4);
                checkArg(split[3].endsWith(".json"));

                idPart = Integer.parseUnsignedInt(split[3].substring(0, split[3].length() - ".json".length()));
                checkIndex(1000, idPart);
                id += idPart;
            } else {
                id = idPart * 1000000L;

                if (split.length == 3) {
                    this.sendIndex(response, path, IntStream.range(0, 1000).mapToObj(i -> PStrings.fastFormat("%03d/", i)));
                    return;
                }

                idPart = Integer.parseUnsignedInt(split[3]);
                checkIndex(1000, idPart);
                id += idPart * 1000L;

                if (split.length == 4) {
                    this.sendIndex(response, path, IntStream.range(0, 1000).mapToObj(i -> PStrings.fastFormat("%03d.json", i)));
                    return;
                }

                checkArg(split.length == 5);
                checkArg(split[4].endsWith(".json"));

                idPart = Integer.parseUnsignedInt(split[4].substring(0, split[4].length() - ".json".length()));
                checkIndex(1000, idPart);
                id += idPart;
            }

            int type = Element.typeId(split[1]);
            long combinedId = Element.addTypeToId(type, id);

            ByteBuffer val = this.storage.externalJsonStorage()[level].get(this.access, combinedId);
            if (val == null) {
                response.status(HttpResponseStatus.NOT_FOUND);
            } else {
                response.status(HttpResponseStatus.OK)
                        .contentType("application/geo+json")
                        .body(Unpooled.wrappedBuffer(val));
            }
        }

        @SneakyThrows(IOException.class)
        private void sendIndex(@NonNull Response response, @NonNull String path, @NonNull Stream<String> values) {
            ByteBuf body = UnpooledByteBufAllocator.DEFAULT.ioBuffer();
            response.contentType("text/html")
                    .status(HttpResponseStatus.OK)
                    .body(body);

            try (PAppendable out = new UTF8ByteBufAppendable(body)) {
                out.appendFmt("<html><body><h1>Index of %s</h1><ul>", '/' + path);

                out.appendFmt("<li><a href=\"%1$s\">%1$s</a></li>", "../");
                values.forEach((IOConsumer<String>) s -> out.appendFmt("<li><a href=\"%1$s\">%1$s</a></li>", s));

                out.appendLn("</ul></body></html>");
            }
        }

        @Override
        public void close() {
            logger.info("Shutting down...");
            this.server.close();
        }
    }
}
