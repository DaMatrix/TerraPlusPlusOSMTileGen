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
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.util.WeightedDouble;
import net.daporkchop.tpposmtilegen.osm.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;

import java.io.File;
import java.util.concurrent.atomic.DoubleAdder;

import static net.daporkchop.lib.common.util.PValidation.*;

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
                LongObjConsumer<Element> func = (id, element) -> {
                    int type = element.type();
                    try {
                        Geometry geometry = element.toGeometry(storage, storage.db().read());
                        if (geometry != null) {
                            WeightedDouble density = geometry.averagePointDensity();
                            value.add(density.value());
                            weight.add(density.weight());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(Element.typeName(type) + ' ' + id, e);
                    }
                    notifier.step(type);
                };

                storage.ways().forEachParallel(storage.db().read(), func);
                storage.relations().forEachParallel(storage.db().read(), func);
                storage.coastlines().forEachParallel(storage.db().read(), func);
            }
        }

        System.out.printf("%s/%s=%s\n", value.sum(), weight.sum(), value.sum() / weight.sum());
    }
}
