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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.osm.Way;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DBAccess;
import net.daporkchop.tpposmtilegen.util.Tile;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class Test implements IMode {
    @Override
    public String name() {
        return "test";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "just debug code";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: test <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        try (Storage storage = new Storage(src.toPath())) {
            try (DBAccess access = storage.db().newTransaction()) {
                long combinedId = Element.addTypeToId(Way.TYPE, 810251999L);
                Element element = storage.getElement(access, combinedId);
                checkState(element != null, "unable to find way!");
                Geometry geometry = element.toGeometry(storage, access);
                checkState(geometry != null, "unable to assemble geometry!");
                System.out.println(geometry);

                long[] tiles = geometry.listIntersectedTiles();
                System.out.println(Arrays.stream(tiles).mapToObj(l -> Tile.tileX(l) + ", " + Tile.tileY(l))
                        .collect(Collectors.joining("], [", "[", "]")));

                long[] tilesDb = storage.intersectedTiles().get(access, combinedId);
                System.out.println(Arrays.stream(tilesDb).mapToObj(l -> Tile.tileX(l) + ", " + Tile.tileY(l))
                        .collect(Collectors.joining("], [", "[", "]")));

                for (long tile : tiles) {
                    LongList elements = new LongArrayList();
                    storage.tileContents().getElementsInTile(access, tile, elements);
                    System.out.println(elements.contains(combinedId));
                }

                storage.convertToGeoJSONAndStoreInDB(access, combinedId, false);

                storage.exportDirtyTiles(access, src.toPath().resolveSibling("switzerland.tiles"));

                access.clear();
            }
        }
    }
}
