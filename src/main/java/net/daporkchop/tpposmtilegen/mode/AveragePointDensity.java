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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleLists;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.util.PArrays;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.tpposmtilegen.geometry.ComplexGeometry;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.geometry.Point;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.Bounds2d;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;

import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class AveragePointDensity implements IMode {
    @Override
    public String name() {
        return "average_point_density";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "Computes the average point density of all geometry elements.";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: assemble_geometry <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        DoubleAdder value = new DoubleAdder();
        DoubleAdder weight = new DoubleAdder();


        try (Storage storage = new Storage(src.toPath())) {
            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Compute average point density")
                    .slot("nodes").slot("ways").slot("relations").slot("coastlines")
                    .build()) {
                /*Path outDir = Paths.get("/media/daporkchop/2tb/aaa");
                PFiles.rmContentsParallel(outDir.toFile());
                Collection<Map.Entry<String, Geometry[]>> geometries = new ConcurrentLinkedQueue<>();
                Bounds2d bb = Bounds2d.of(Point.doubleToFix(8.58108d), Point.doubleToFix(8.55190d), Point.doubleToFix(47.22108d), Point.doubleToFix(47.20743d));*/

                LongObjConsumer<Element> func = (id, element) -> {
                    int type = element.type();
                    try {
                        Geometry geometry = element.toGeometry(storage, storage.db().read());
                        if (geometry != null) {
                            WeightedDouble density = geometry.averagePointDensity();
                            value.add(density.value());
                            weight.add(density.weight());
                        }

                        /*if (geometry != null && bb.intersects(geometry.bounds())) {
                            geometries.add(new AbstractMap.SimpleEntry<>(Element.typeName(type) + "/" + element.id(), IntStream.rangeClosed(0, 2).mapToObj(geometry::simplifyTo).filter(Objects::nonNull).toArray(Geometry[]::new)));
                        }*/

                        /*Geometry simplified1;
                        Geometry simplified2;
                        if (false && geometry != null && (simplified1 = geometry.simplifyTo(1)) != null) {
                            simplified2 = geometry.simplifyTo(2);

                            StringBuilder builder = new StringBuilder();

                            builder.setLength(0);
                            builder.append("{\"type\":\"Feature\",\"properties\":{\"level\":\"0\"},\"geometry\":");
                            geometry.toGeoJSON(builder);
                            builder.append("}\n");
                            Files.write(outDir.resolve(id + "_0.json"), builder.toString().getBytes(StandardCharsets.UTF_8));

                            builder.setLength(0);
                            builder.append("{\"type\":\"Feature\",\"properties\":{\"level\":\"1\"},\"geometry\":");
                            simplified1.toGeoJSON(builder);
                            builder.append("}\n");
                            Files.write(outDir.resolve(id + "_1.json"), builder.toString().getBytes(StandardCharsets.UTF_8));

                            if (simplified2 != null) {
                                builder.setLength(0);
                                builder.append("{\"type\":\"Feature\",\"properties\":{\"level\":\"2\"},\"geometry\":");
                                simplified2.toGeoJSON(builder);
                                builder.append("}\n");
                                Files.write(outDir.resolve(id + "_2.json"), builder.toString().getBytes(StandardCharsets.UTF_8));
                            }
                        }*/
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                    notifier.step(type);
                };

                storage.ways().forEachParallel(storage.db().read(), func);
                storage.relations().forEachParallel(storage.db().read(), func);
                storage.coastlines().forEachParallel(storage.db().read(), func);

                /*StringJoiner[] joiners = PArrays.filled(3, StringJoiner[]::new, () -> new StringJoiner(",\n", "{\"type\":\"FeatureCollection\",\"features\":[\n", "]}\n"));
                StringBuilder builder = new StringBuilder();
                geometries.forEach(e -> {
                    for (int i = 0; i < e.getValue().length; i++) {
                        builder.setLength(0);
                        builder.append("{\"type\":\"Feature\",\"properties\":{\"id\":\"").append(e.getKey()).append("\"},\"geometry\":");
                        e.getValue()[i].toGeoJSON(builder);
                        builder.append('}');
                        joiners[i].add(builder);
                    }
                });

                for (int i = 0; i < joiners.length; i++) {
                    Files.write(outDir.resolve(i + ".json"), joiners[i].toString().getBytes(StandardCharsets.UTF_8));
                }*/
            }
        }

        logger.info("%s/%s=%s", value.sum(), weight.sum(), value.sum() / weight.sum());
    }
}
