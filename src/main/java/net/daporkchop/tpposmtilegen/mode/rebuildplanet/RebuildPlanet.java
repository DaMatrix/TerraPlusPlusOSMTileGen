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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.NonNull;
import net.daporkchop.lib.binary.oio.appendable.PAppendable;
import net.daporkchop.lib.binary.oio.writer.UTF8FileWriter;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.common.misc.string.PUnsafeStrings;
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Geometry;
import net.daporkchop.tpposmtilegen.osm.area.Area;
import net.daporkchop.tpposmtilegen.osm.line.Line;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.Threading;
import net.daporkchop.tpposmtilegen.util.Tile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Locale;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.tpposmtilegen.util.Tile.*;

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
            try (ProgressNotifier notifier = new ProgressNotifier("Nuke tile directory: ", 5000L, "files", "directories")) {
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

        try (Storage storage = new Storage(src.toPath())) {
            storage.purge(true, true); //clear everything

            try (ProgressNotifier notifier = new ProgressNotifier("Build references: ", 5000L, "ways", "relations")) {
                Threading.forEachParallelLong(combinedId -> {
                    int type = Element.extractType(combinedId);
                    String typeName = Element.typeName(type);
                    long id = Element.extractId(combinedId);
                    try {
                        Element element = storage.getElement(combinedId);
                        checkState(element != null, "unknown %s %d", typeName, id);
                        element.computeReferences(storage);
                    } catch (Exception e) {
                        throw new RuntimeException(typeName + ' ' + id, e);
                    }
                    notifier.step(type - 1);
                }, storage.spliterateElements(false, true, true));
                storage.flush();
            }

            try (ProgressNotifier notifier = new ProgressNotifier("Assemble & index geometry: ", 5000L, "nodes", "lines", "areas")) {
                Threading.forEachParallelLong(combinedId -> {
                    int type = Element.extractType(combinedId);
                    String typeName = Element.typeName(type);
                    long id = Element.extractId(combinedId);
                    try {
                        Element element = storage.getElement(combinedId);
                        checkState(element != null, "unknown %s %d", typeName, id);
                        Geometry geometry = element.toGeometry(storage);
                        if (geometry != null) {
                            Bounds2d bounds = geometry.computeObjectBounds();
                            int tileMinX = coord_point2tile(bounds.minX());
                            int tileMaxX = coord_point2tile(bounds.maxX());
                            int tileMinY = coord_point2tile(bounds.minY());
                            int tileMaxY = coord_point2tile(bounds.maxY());
                            long[] arr = new long[(tileMaxX - tileMinX + 1) * (tileMaxY - tileMinY + 1)];
                            for (int i = 0, x = tileMinX; x <= tileMaxX; x++) {
                                for (int y = tileMinY; y <= tileMaxY; y++) {
                                    storage.dirtyTiles().set(arr[i] = xy2tilePos(x, y));
                                }
                            }
                            storage.tiles().addElementToTiles(LongArrayList.wrap(arr), type, id); //add this element to all tiles
                            storage.tileCounts().setTileCount(type, id, arr.length); //keep track of how many tiles this element is stored in

                            try (Handle<StringBuilder> handle = PorkUtil.STRINGBUILDER_POOL.get()) { //encode geometry to GeoJSON
                                StringBuilder builder = handle.get();
                                builder.setLength(0);

                                geometry.toGeoJSON(builder);

                                if (arr.length == 1) { //element is only referenced once, store GeoJSON data in db and exit
                                    storage.tempJsonStorage().put(combinedId, Arrays.copyOf(PUnsafeStrings.unwrap(builder), builder.length()));
                                } else { //element is referenced multiple times, store it in a separate file
                                    File file = new File(dst, PStrings.fastFormat("%s/%02d/%02d/%d.json", Element.typeName(type), id % 100L, (id / 100L) % 100L, id));
                                    Files.createDirectories(file.getParentFile().toPath());
                                    try (PAppendable out = new UTF8FileWriter(new FileOutputStream(file), "", false)) {
                                        out.append(builder);
                                    }
                                    if (geometry instanceof Line) {
                                        notifier.step(1);
                                    } else if (geometry instanceof Area) {
                                        notifier.step(2);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(typeName + ' ' + id, e);
                    }
                    notifier.step(type);
                }, storage.spliterateElements(true, true, true));
                storage.flush();
            }

            try (ProgressNotifier notifier = new ProgressNotifier("Write tiles: ", 5000L, "tiles")) {
                Threading.forEachParallelLong(tilePos -> {
                    int tileX = Tile.tileX(tilePos);
                    int tileY = Tile.tileY(tilePos);
                    try {
                        LongList elements = new LongArrayList();
                        storage.tiles().getElementsInTile(tilePos, elements);
                        if (elements.isEmpty()) { //nothing to render
                            return;
                        }

                        LongList counts = storage.tileCounts().getTileCounts(elements);
                        LongList idsDirect = new LongArrayList();
                        LongList idsReference = new LongArrayList();

                        for (int i = 0, size = elements.size(); i < size; i++) {
                            long count = counts.getLong(i);
                            if (count == 1L) { //element has only a single reference, so we can store the entire thing in this tile
                                idsDirect.add(elements.getLong(i));
                            } else if (count > 1L) { //element has multiple references, so we'll store it in the tile as a reference
                                idsReference.add(elements.getLong(i));
                            } else {
                                throw new IllegalStateException();
                            }
                        }

                        File file = new File(dst, PStrings.fastFormat("tile/%d/%d.json", tileX, tileY));
                        Files.createDirectories(file.getParentFile().toPath());
                        try (PAppendable out = new UTF8FileWriter(new FileOutputStream(file), "", false)) {
                            storage.tempJsonStorage().getAll(idsDirect).forEach((IOConsumer<char[]>) arr -> out.append(CharBuffer.wrap(arr)));

                            if (!idsReference.isEmpty()) {
                                try (Handle<StringBuilder> handle = PorkUtil.STRINGBUILDER_POOL.get()) {
                                    StringBuilder builder = handle.get();
                                    Formatter fmt = new Formatter(builder, Locale.US);

                                    for (int i = 0, size = idsReference.size(); i < size; i++) {
                                        builder.setLength(0);

                                        long combinedId = idsReference.getLong(i);
                                        int type = Element.extractType(combinedId);
                                        long id = Element.extractId(combinedId);
                                        builder.append("{\"type\":\"Reference\",\"location\":\"").append(Element.typeName(type)).append('/');
                                        fmt.format("%02d/%02d/%d", id % 100L, (id / 100L) % 100L, id);
                                        builder.append(".json\"}\n");
                                        out.append(builder);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("tile " + tilePos, e);
                    }
                    notifier.step(0);
                }, storage.dirtyTiles().spliterator());
                storage.dirtyTiles().clear();
                storage.flush();
            }

            storage.purge(true, false); //erase temporary data
        }
    }
}
