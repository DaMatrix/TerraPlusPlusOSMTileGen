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

package net.daporkchop.tpposmtilegen.storage;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.disk.DiskIndex;
import com.googlecode.cqengine.persistence.disk.DiskPersistence;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.io.File;

import static net.daporkchop.tpposmtilegen.storage.OSMAttributes.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Storage {
    public IndexedCollection<Element> openStorage(@NonNull File root) {
        IndexedCollection<Element> storage = new ConcurrentIndexedCollection<>(DiskPersistence.onPrimaryKeyInFile(ID, new File(root, "index.dat")));
        //storage.addIndex(DiskIndex.onAttribute(MIN_X));
        //storage.addIndex(DiskIndex.onAttribute(MAX_X));
        //storage.addIndex(DiskIndex.onAttribute(MIN_Z));
        //storage.addIndex(DiskIndex.onAttribute(MAX_Z));
        //storage.addIndex(DiskIndex.onAttribute(CHILDREN));
        //storage.addIndex(DiskIndex.onAttribute(TAGS));
        return storage;
    }
}
