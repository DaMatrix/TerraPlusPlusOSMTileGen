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

package net.daporkchop.tpposmtilegen.storage.rocksdb.access;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * @author DaPorkchop_
 */
public interface DBIterator extends AutoCloseable {
    boolean isValid();

    void seekToFirst();

    void seekToLast();

    void seekCeil(@NonNull byte[] key);

    void seekFloor(@NonNull byte[] key);

    void next();

    void prev();

    byte[] key();

    byte[] value();

    @Override
    void close() throws Exception;

    /**
     * @author DaPorkchop_
     */
    @RequiredArgsConstructor
    class SimpleRocksIteratorWrapper implements DBIterator {
        @NonNull
        protected final RocksIterator delegate;

        @Override
        public boolean isValid() {
            return this.delegate.isValid();
        }

        @Override
        public void seekToFirst() {
            this.delegate.seekToFirst();
        }

        @Override
        public void seekToLast() {
            this.delegate.seekToLast();
        }

        @Override
        public void seekCeil(@NonNull byte[] key) {
            this.delegate.seek(key);
        }

        @Override
        public void seekFloor(@NonNull byte[] key) {
            this.delegate.seekForPrev(key);
        }

        @Override
        public void next() {
            this.delegate.next();
        }

        @Override
        public void prev() {
            this.delegate.prev();
        }

        @Override
        public byte[] key() {
            return this.delegate.key();
        }

        @Override
        public byte[] value() {
            return this.delegate.value();
        }

        @Override
        public void close() throws Exception {
            this.delegate.close();
        }
    }

    /**
     * @author DaPorkchop_
     */
    class SimpleRangedRocksIteratorWrapper extends SimpleRocksIteratorWrapper {
        public static DBIterator from(@NonNull RocksDB db, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ReadOptions baseReadOptions, @NonNull byte[] fromInclusive, @NonNull byte[] toExclusive) throws Exception {
            Slice fromInclusiveSlice = null;
            Slice toExclusiveSlice = null;
            ReadOptions readOptions = null;
            try {
                fromInclusiveSlice = new Slice(fromInclusive);
                toExclusiveSlice = new Slice(toExclusive);
                readOptions = new ReadOptions(baseReadOptions)
                        .setIterateLowerBound(fromInclusiveSlice)
                        .setIterateUpperBound(toExclusiveSlice);

                return new SimpleRangedRocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions), readOptions, fromInclusiveSlice, toExclusiveSlice);
            } catch (Exception e) {
                if (readOptions != null) {
                    readOptions.close();
                }
                if (toExclusiveSlice != null) {
                    toExclusiveSlice.close();
                }
                if (fromInclusiveSlice != null) {
                    fromInclusiveSlice.close();
                }
                throw e;
            }
        }

        protected final ReadOptions readOptions;
        protected final Slice fromInclusive;
        protected final Slice toExclusive;

        public SimpleRangedRocksIteratorWrapper(@NonNull RocksIterator delegate, @NonNull ReadOptions readOptions, @NonNull Slice fromInclusive, @NonNull Slice toExclusive) {
            super(delegate);

            this.readOptions = readOptions;
            this.fromInclusive = fromInclusive;
            this.toExclusive = toExclusive;
        }

        @Override
        public void close() throws Exception {
            super.close();

            this.readOptions.close();
            this.fromInclusive.close();
            this.toExclusive.close();
        }
    }
}
