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
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
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

    /**
     * Gets an instance of {@link NativeRocksHelper.KeyValueSlice} which refers to the current key and value.
     * <p>
     * The returned instance's contents will become invalid as soon as this iterator moves to a different entry or is closed.
     *
     * @return an instance of {@link NativeRocksHelper.KeyValueSlice} which refers to the current key and value
     */
    NativeRocksHelper.KeyValueSlice keyValueSlice();

    @Override
    void close() throws Exception;

    /**
     * @author DaPorkchop_
     */
    @RequiredArgsConstructor
    class SimpleRocksIteratorWrapper extends NativeRocksHelper.KeyValueSlice implements DBIterator {
        @NonNull
        protected final RocksIterator delegate;

        protected boolean sliceValid = false;

        @Override
        public boolean isValid() {
            return this.delegate.isValid();
        }

        @Override
        public void seekToFirst() {
            this.sliceValid = false;
            this.delegate.seekToFirst();
        }

        @Override
        public void seekToLast() {
            this.sliceValid = false;
            this.delegate.seekToLast();
        }

        @Override
        public void seekCeil(@NonNull byte[] key) {
            this.sliceValid = false;
            this.delegate.seek(key);
        }

        @Override
        public void seekFloor(@NonNull byte[] key) {
            this.sliceValid = false;
            this.delegate.seekForPrev(key);
        }

        @Override
        public void next() {
            this.sliceValid = false;
            this.delegate.next();
        }

        @Override
        public void prev() {
            this.sliceValid = false;
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
        public NativeRocksHelper.KeyValueSlice keyValueSlice() {
            if (!this.sliceValid) {
                this.sliceValid = true;
                NativeRocksHelper.getKeyAndValueAsView(this.delegate, this);
            }
            return this;
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
        public static DBIterator from(@NonNull RocksDB db, @NonNull ColumnFamilyHandle columnFamilyHandle, @NonNull ReadOptions baseReadOptions, byte[] fromInclusive, byte[] toExclusive) throws Exception {
            if (fromInclusive == null && toExclusive == null) {
                return new SimpleRocksIteratorWrapper(db.newIterator(columnFamilyHandle, baseReadOptions));
            }

            Slice fromInclusiveSlice = null;
            Slice toExclusiveSlice = null;
            ReadOptions readOptions = null;
            try {
                fromInclusiveSlice = fromInclusive != null ? new Slice(fromInclusive) : null;
                toExclusiveSlice = toExclusive != null ? new Slice(toExclusive) : null;
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

        public SimpleRangedRocksIteratorWrapper(@NonNull RocksIterator delegate, @NonNull ReadOptions readOptions, Slice fromInclusive, Slice toExclusive) {
            super(delegate);

            this.readOptions = readOptions;
            this.fromInclusive = fromInclusive;
            this.toExclusive = toExclusive;
        }

        @Override
        public void close() throws Exception {
            super.close();

            this.readOptions.close();
            if (this.fromInclusive != null) {
                this.fromInclusive.close();
            }
            if (this.toExclusive != null) {
                this.toExclusive.close();
            }
        }
    }
}
