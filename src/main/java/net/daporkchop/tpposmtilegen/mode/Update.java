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

import com.sun.net.httpserver.HttpServer;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.Tile;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

/**
 * @author DaPorkchop_
 */
public class Update implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: update <index_dir> <tile_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        Path dst = Paths.get(args[1]);

        try (Storage storage = new Storage(src.toPath())) {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/tile/", exchange -> {
                try {
                    Matcher matcher = Pattern.compile("^/tile/(\\d+)/(\\d+)\\.json$").matcher(exchange.getRequestURI().getPath());
                    checkArg(matcher.find());
                    int tileX = Integer.parseInt(matcher.group(1));
                    int tileY = Integer.parseInt(matcher.group(2));
                    long tilePos = xy2tilePos(tileX, tileY);

                    LongList elements = new LongArrayList();
                    storage.tileContents().getElementsInTile(tilePos, elements);

                    ByteBuffer[] buffers = storage.tempJsonStorage().getAll(elements).toArray(new ByteBuffer[0]);
                    exchange.sendResponseHeaders(200, 0);

                    try (OutputStream out = exchange.getResponseBody()) {
                        for (ByteBuffer buffer : buffers) {
                            out.write(buffer.array(), buffer.arrayOffset(), buffer.remaining());
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(exchange.getRequestURI().getPath(), e);
                }
            });
            server.createContext("/", exchange -> {
                Path path = dst.resolve(exchange.getRequestURI().getPath().substring(1));
                if (Files.isRegularFile(path)) {
                    exchange.sendResponseHeaders(200, 0);
                    try (InputStream in = new FileInputStream(path.toFile());
                         OutputStream out = exchange.getResponseBody()) {
                        byte[] buf = new byte[4096];
                        for (int i; (i = in.read(buf)) > 0; ) {
                            out.write(buf, 0, i);
                        }
                    }
                } else {
                    exchange.sendResponseHeaders(404, 0);
                    exchange.getResponseBody().close();
                }
            });
            server.start();

            new Scanner(System.in).nextLine();
            server.stop(5000);
        }
    }
}
