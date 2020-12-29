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

package net.daporkchop.tpposmtilegen.mode.testindex;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.query.Query;
import lombok.NonNull;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.storage.Element;
import net.daporkchop.tpposmtilegen.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.stream.Stream;

import static com.googlecode.cqengine.query.QueryFactory.*;
import static net.daporkchop.tpposmtilegen.storage.OSMAttributes.*;

/**
 * @author DaPorkchop_
 */
public class TestIndex implements IMode {
    @Override
    public void run(@NonNull String... args) throws IOException {
        IndexedCollection<Element> storage = Storage.openStorage(new File(args[0]));

        Query<Element> query;
        query = startsWith(TAGS, "natural=");
        query = and(startsWith(TAGS, "natural"), startsWith(TAGS, "water"));

        try (Stream<Element> stream = storage.retrieve(query).stream()) {
            stream.forEach(System.out::println);
        }
    }
}
