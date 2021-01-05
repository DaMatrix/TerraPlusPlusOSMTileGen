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

import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.tpposmtilegen.geometry.Shape;
import net.daporkchop.tpposmtilegen.osm.Coastline;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.Feature;

import java.io.File;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class DigestCoastlines implements IMode {
    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 2, "Usage: digest_coastlines <shapefile> <index_dir>");
        File src = PFiles.assertFileExists(new File(args[0]));
        File dst = new File(args[1]);

        try (ProgressNotifier notifier = new ProgressNotifier("Digest coastlines: ", 5000L, "pieces");
             Storage storage = new Storage(dst.toPath())) {
            storage.coastlines().clear();

            FileDataStore store = FileDataStoreFinder.getDataStore(src);
            SimpleFeatureSource featureSource = store.getFeatureSource();

            FeatureIterator itr = featureSource.getFeatures().features();
            long id = 0L;
            while (itr.hasNext()) {
                Feature feature = itr.next();
                int x = (Integer) feature.getProperty("x").getValue();
                int y = (Integer) feature.getProperty("y").getValue();
                MultiPolygon mp = (MultiPolygon) feature.getProperty((String) null).getValue();

                Coastline.Area area = this.toArea(mp);
                storage.coastlines().put(id, new Coastline(id, area));
                id++;
                notifier.step(0);
            }
            storage.coastlineCount().set(id);
        }
    }

    protected Coastline.Area toArea(MultiPolygon multiPolygon) {
        return new Coastline.Area(IntStream.range(0, multiPolygon.getNumGeometries())
                .mapToObj(multiPolygon::getGeometryN)
                .map(Polygon.class::cast)
                .map(this::toShape)
                .toArray(Shape[]::new));
    }

    protected Shape toShape(Polygon polygon) {
        Point[] outer = this.toLine(polygon.getExteriorRing());
        Point[][] inner = IntStream.range(0, polygon.getNumInteriorRing())
                .mapToObj(polygon::getInteriorRingN)
                .map(this::toLine)
                .toArray(Point[][]::new);
        return new Shape(outer, inner);
    }

    protected Point[] toLine(LinearRing ring) {
        checkState(ring.isClosed(), "ring isn't closed!");
        Point[] line = IntStream.range(0, ring.getNumPoints())
                .mapToObj(ring::getPointN)
                .map(point -> new Point(point.getX(), point.getY()))
                .toArray(Point[]::new);
        checkState(line[0].equals(line[line.length - 1]), "processed ring isn't closed!");
        return line;
    }
}
