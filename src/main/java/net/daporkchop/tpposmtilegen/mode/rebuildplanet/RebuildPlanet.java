/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.mode.rebuildplanet;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.NonNull;
import net.daporkchop.lib.binary.oio.appendable.PAppendable;
import net.daporkchop.lib.binary.oio.writer.UTF8FileWriter;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Node;
import net.daporkchop.tpposmtilegen.osm.Relation;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.TileDB;
import net.daporkchop.tpposmtilegen.util.Point;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Threading;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.StreamSupport;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class RebuildPlanet implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: rebuild_planet <index_dir> <tile_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));
        File dst = new File(args[1]);
        if (PFiles.checkDirectoryExists(dst)) {
            try (ProgressNotifier notifier = new ProgressNotifier(" Nuke tile directory: ", 5000L, "files", "directories")) {
                Files.walkFileTree(dst.toPath(), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        notifier.step(0);
                        return super.visitFile(file, attrs);
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        notifier.step(1);
                        return super.postVisitDirectory(dir, exc);
                    }
                });
            }
        }
        PFiles.ensureDirectoryExists(dst);

        try (Storage storage = new Storage(src.toPath())) {
            System.out.println("Clearing references... (this might take a moment)");
            storage.references().clear();
            System.out.println("Clearing tile index...");
            //storage.tiles().clear();
            System.out.println("Cleared.");

            /*try (ProgressNotifier notifier = new ProgressNotifier(" Build references: ", 5000L, "ways", "relations")) {
                Threading.forEachParallelLong(storage.wayFlags().spliterator(), id -> {
                    try {
                        Way way = storage.ways().get(id);
                        checkArg(way != null, "unknown way: %d", id);
                        way.computeReferences(storage);
                    } catch (Exception e) {
                        throw new RuntimeException("way " + id, e);
                    }
                    notifier.step(0);
                });
                Threading.forEachParallelLong(storage.relationFlags().spliterator(), id -> {
                    try {
                        Relation relation = storage.relations().get(id);
                        checkArg(relation != null, "unknown relation: %d", id);
                        relation.computeReferences(storage);
                    } catch (Exception e) {
                        throw new RuntimeException("relation " + id, e);
                    }
                    notifier.step(1);
                });
            }
            storage.flush();*/

            /*try (ProgressNotifier notifier = new ProgressNotifier(" Build tile index: ", 5000L, "nodes", "ways", "relations")) {
                Threading.forEachParallelLong(storage.taggedNodeFlags().spliterator(), id -> {
                    try {
                        Node node = storage.nodes().get(id);

                        long tilePos = TileDB.toTilePosition(node.point().x(), node.point().y());
                        storage.tiles().addElementToTiles(LongLists.singleton(tilePos), Node.TYPE, id);
                        storage.dirtyTiles().set(tilePos);
                    } catch (Exception e) {
                        throw new RuntimeException("node " + id, e);
                    }
                    notifier.step(0);
                });
            }
            storage.flush();*/

            try (ProgressNotifier notifier = new ProgressNotifier(" Render tiles: ", 5000L, "tiles")) {
                Threading.forEachParallelLong(PorkUtil.CPU_COUNT << 1, storage.dirtyTiles().spliterator(), tilePos -> {
                    int tileX = TileDB.extractTileX(tilePos);
                    int tileY = TileDB.extractTileY(tilePos);
                    try {
                        LongList elements = new LongArrayList();
                        storage.tiles().getElementsInTile(tilePos, elements);
                        if (elements.isEmpty()) { //nothing to render
                            return;
                        }

                        LongList nodeIds = new LongArrayList();
                        for (int i = 0, size = elements.size(); i < size; i++) {
                            long combined = elements.getLong(i);
                            switch (Element.extractType(combined)) {
                                case Node.TYPE:
                                    nodeIds.add(Element.extractId(combined));
                                    break;
                                default:
                                    throw new IllegalArgumentException("unsupported element type: " + Element.extractType(combined));
                            }
                        }

                        try (Handle<StringBuilder> handle = PorkUtil.STRINGBUILDER_POOL.get()) {
                            StringBuilder builder = handle.get();
                            builder.setLength(0);

                            if (!nodeIds.isEmpty()) {
                                List<Node> nodes = storage.nodes().getAll(nodeIds);
                                for (int i = 0, size = nodes.size(); i < size; i++) {
                                    Node node = nodes.get(i);
                                    checkState(node != null, "unknown node %d", nodeIds.getLong(i));
                                    node.toGeoJSON(storage, builder);
                                }
                            }

                            File file = new File(dst, PStrings.fastFormat("tiles/%d/%d.json", tileX, tileY));
                            try (PAppendable out = new UTF8FileWriter(PFiles.ensureFileExists(file))) {
                                out.append(builder);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("tile " + tilePos, e);
                    }
                    notifier.step(0);
                });
                storage.dirtyTiles().clear();
            }
            storage.flush();
        }
    }
}
