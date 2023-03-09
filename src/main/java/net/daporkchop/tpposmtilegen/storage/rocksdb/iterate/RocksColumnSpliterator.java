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

package net.daporkchop.tpposmtilegen.storage.rocksdb.iterate;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.annotation.ThreadSafe;
import net.daporkchop.tpposmtilegen.natives.NativeRocksHelper;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.util.Utils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LevelMetaData;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileMetaData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class RocksColumnSpliterator implements Spliterator<NativeRocksHelper.KeyValueSlice>, AutoCloseable {
    protected final RocksColumnSpliterator root;
    protected final RocksColumnSpliterator parent;
    protected final Database database;
    protected final ColumnFamilyHandle column;

    protected final Comparator<byte[]> heapKeyComparator = Utils.BYTES_COMPARATOR;
    protected final Comparator<NativeRocksHelper.KeyValueSlice> sliceKeyComparator = NativeRocksHelper.KeyValueSlice.lexOrderKeyComparator();
    protected final KeyOperations<byte[]> keyOperations;

    protected final boolean rootOwnsSnapshot;
    protected final Snapshot snapshot;

    protected final Options options;
    protected final ReadOptions readOptions;

    protected final List<SstFileMetaData> level0Files;
    protected final List<List<SstFileMetaData>> sortedNonzeroLevelFiles;

    protected final List<WrappedSstFile> wrappedSstFiles;

    protected final byte[] totalSmallestKey;
    protected final byte[] totalLargestKey;
    protected final IntervalTree<byte[], WrappedSstFile> intervalTree;

    @Getter
    protected byte[] smallestKeyInclusive;
    @Getter
    protected byte[] largestKeyInclusive;
    @Getter
    protected byte[] largestKeyExclusive; //may be null
    protected BoundedReadOptions boundedReadOptions;
    protected RocksIterator iterator;
    protected NativeRocksHelper.KeyValueSlice cachedKeyValueSlice;
    protected boolean reachedEnd = false;

    protected List<RocksColumnSpliterator> children = new ArrayList<>();

    public RocksColumnSpliterator(@NonNull Database database, @NonNull ColumnFamilyHandle column, @NonNull Optional<Snapshot> snapshot, @NonNull DatabaseConfig.ReadType readType, @NonNull KeyOperations<byte[]> keyOperations) throws RocksDBException {
        this.root = this;
        this.parent = null;
        this.database = database;
        this.column = column;

        this.keyOperations = keyOperations;

        if (snapshot.isPresent()) {
            this.rootOwnsSnapshot = false;
            this.snapshot = snapshot.get();
        } else {
            this.rootOwnsSnapshot = true;
            if (!database.config().readOnly()) { //make sure all data is written out to SST files
                database.delegate().flushWal(false);
                database.delegate().flush(database.config().flushOptions(DatabaseConfig.FlushType.GENERAL), column);
            }
            this.snapshot = database.delegate().getSnapshot();
        }

        this.options = new Options(database.config().dbOptions(), database.columns().get(column).getOptions());
        this.readOptions = new ReadOptions(database.config().readOptions(readType)).setSnapshot(this.snapshot);

        //determine the total minimum and maximum keys
        try (RocksIterator iterator = database.delegate().newIterator(column, this.readOptions)) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                this.totalSmallestKey = iterator.key();
                iterator.seekToLast();
                checkState(iterator.isValid());
                this.totalLargestKey = iterator.key();
            } else { //empty
                this.totalSmallestKey = null;
                this.totalLargestKey = null;
                this.reachedEnd = true;
            }
        }

        List<LevelMetaData> levelMetas = database.delegate().getColumnFamilyMetaData(column).levels();

        checkState(levelMetas.get(0).level() == 0);
        this.level0Files = levelMetas.get(0).files().stream()
                .peek(fileMeta -> checkState(fileMeta.numDeletions() == 0L, "file '%s' contains %d deletions!", fileMeta.fileName(), fileMeta.numDeletions()))
                .sorted(Comparator.comparingLong(SstFileMetaData::smallestSeqno))
                .collect(Collectors.toList());

        for (int i = 1; i < this.level0Files.size(); i++) { //make sure the level-0 file's sequence numbers don't overlap
            SstFileMetaData prev = this.level0Files.get(i - 1);
            SstFileMetaData curr = this.level0Files.get(i);
            checkState(prev.largestSeqno() < curr.smallestSeqno(), "level-0 file '%s' has overlapping sequence numbers with '%s'!", prev.fileName(), curr.fileName());
        }

        this.sortedNonzeroLevelFiles = levelMetas.stream()
                .skip(1L) //exclude level-0, as we want to preserve its order!
                .filter(levelMeta -> levelMeta.size() != 0L)
                .sorted(Comparator.comparingInt(LevelMetaData::level))
                .peek(levelMeta -> checkState(this.canBeSortedSequence(levelMeta.files()), "not a sorted key sequence: %d", levelMeta.level()))
                .map(levelMeta -> levelMeta.files().stream()
                        .sorted(Comparator.comparing(SstFileMetaData::smallestKey, this.heapKeyComparator))
                        .peek(fileMeta -> checkState(fileMeta.numDeletions() == 0L, "file '%s' contains %d deletions!", fileMeta.fileName(), fileMeta.numDeletions()))
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());

        if (this.canBeSortedSequence(this.level0Files)) {
            //if the files at level-0 could be a sorted sequence, we know that none of them overlap each other, and can therefore be treated as an ordinary level
            this.sortedNonzeroLevelFiles.add(0, new ArrayList<>(this.level0Files));
            this.level0Files.clear();
        }

        this.wrappedSstFiles = new ArrayList<>(this.level0Files.size() + this.sortedNonzeroLevelFiles.stream().mapToInt(List::size).sum());
        {
            int priority = 0;

            //add level-0 files with priority by order of their age (newest is highest-priority)
            for (int i = this.level0Files.size() - 1; i >= 0; i--) {
                this.wrappedSstFiles.add(new WrappedSstFile(this.level0Files.get(i), priority++));
            }

            //add files for remaining levels all at once. they get to have the same priorities as they don't overlap
            for (List<SstFileMetaData> level : this.sortedNonzeroLevelFiles) {
                for (SstFileMetaData fileMeta : level) {
                    this.wrappedSstFiles.add(new WrappedSstFile(fileMeta, priority));
                }
                priority++;
            }
        }

        this.intervalTree = new IntervalTree<>(this.heapKeyComparator, keyOperations, this.wrappedSstFiles);

        this.smallestKeyInclusive = this.totalSmallestKey;
        this.largestKeyInclusive = this.totalLargestKey;
        this.largestKeyExclusive = null;
    }

    private RocksColumnSpliterator(@NonNull RocksColumnSpliterator parent, @NonNull byte[] smallestKeyInclusive, @NonNull byte[] largestKeyInclusive, byte[] largestKeyExclusive) {
        this.root = parent.root;
        this.parent = parent;
        this.database = parent.database;
        this.column = parent.column;

        this.keyOperations = parent.keyOperations;

        this.rootOwnsSnapshot = parent.rootOwnsSnapshot;
        this.snapshot = parent.snapshot;

        this.options = parent.options;
        this.readOptions = parent.readOptions;

        this.level0Files = parent.level0Files;
        this.sortedNonzeroLevelFiles = parent.sortedNonzeroLevelFiles;

        this.wrappedSstFiles = parent.wrappedSstFiles;

        this.totalSmallestKey = parent.totalSmallestKey;
        this.totalLargestKey = parent.totalLargestKey;
        this.intervalTree = parent.intervalTree;

        this.smallestKeyInclusive = smallestKeyInclusive;
        this.largestKeyInclusive = largestKeyInclusive;
        this.largestKeyExclusive = largestKeyExclusive;
    }

    protected boolean canBeSortedSequence(@NonNull List<SstFileMetaData> unsortedFileMetas) {
        List<SstFileMetaData> sortedFileMetas = new ArrayList<>(unsortedFileMetas);
        sortedFileMetas.sort(Comparator.comparing(SstFileMetaData::smallestKey, this.heapKeyComparator));
        for (int i = 1; i < sortedFileMetas.size(); i++) {
            SstFileMetaData prev = sortedFileMetas.get(i - 1);
            SstFileMetaData curr = sortedFileMetas.get(i);
            if (this.heapKeyComparator.compare(prev.largestKey(), curr.smallestKey()) >= 0) {
                return false;
            }
        }
        return true;
    }

    protected boolean isIteratorInitialized() {
        return this.iterator != null;
    }

    protected void initializeIterator() {
        checkState(!this.reachedEnd);
        checkState(this.boundedReadOptions == null && this.iterator == null && this.cachedKeyValueSlice == null);
        this.boundedReadOptions = new BoundedReadOptions(this.readOptions, this.smallestKeyInclusive, this.largestKeyExclusive);
        this.iterator = this.database.delegate().newIterator(this.column, this.boundedReadOptions.readOptions);
        this.cachedKeyValueSlice = new NativeRocksHelper.KeyValueSlice();
    }

    protected void resetIterator(boolean reachedEnd) {
        checkState(!this.reachedEnd);
        this.reachedEnd = reachedEnd;

        try (BoundedReadOptions boundedReadOptions = this.boundedReadOptions;
             RocksIterator iterator = this.iterator) {
            checkState(this.boundedReadOptions != null && this.iterator != null && this.cachedKeyValueSlice != null);
            this.boundedReadOptions = null;
            this.iterator = null;
            this.cachedKeyValueSlice = null;
        }
    }

    @Override
    public boolean tryAdvance(@NonNull Consumer<? super NativeRocksHelper.KeyValueSlice> action) {
        if (this.reachedEnd) {
            return false;
        }

        if(!this.isIteratorInitialized()) {
            this.initializeIterator();
            this.iterator.seekToFirst();
        } else {
            this.iterator.next();
        }

        if (!this.iterator.isValid()) {
            this.resetIterator(true);
            return false;
        }

        NativeRocksHelper.getKeyAndValueAsView(this.iterator, this.cachedKeyValueSlice);
        action.accept(this.cachedKeyValueSlice);
        return true;
    }

    @Override
    public RocksColumnSpliterator trySplit() {
        if (this.reachedEnd || this.estimateSize() <= 512L) {
            return null;
        }

        byte[] lo;
        byte[] hi = this.largestKeyInclusive;
        byte[] mid = null;

        if (this.isIteratorInitialized()) { //we've already advanced a bit, skip ahead to the next key
            this.iterator.next();
            if (!this.iterator.isValid()) { //reached the end
                this.resetIterator(true);
                return null;
            }
            lo = this.iterator.key();
            this.iterator.prev();
            checkState(this.iterator.isValid());
        } else { //we haven't actually started iteration, we can use the iteration lower bound
            lo = this.smallestKeyInclusive;
        }

        //binary search for a middle value which yields an approximately even weight
        double t = 0.5d;
        double add = 0.25d;
        for (int i = 0; i < 16; i++, add *= 0.5d) {
            mid = this.keyOperations.lerp(lo, hi, t);

            long leftWeight = this.intervalTree.getTotalWeightInInclusiveRange(lo, mid).orElse(0L);
            long rightWeight = this.intervalTree.getTotalWeightInInclusiveRange(this.keyOperations.increment(mid).get(), hi).orElse(0L);
            if (leftWeight < rightWeight) {
                t += add;
            } else {
                t -= add;
            }
        }

        if (this.heapKeyComparator.compare(lo, mid) >= 0) {
            return null;
        } else {
            byte[] afterMid = this.keyOperations.increment(mid).get();

            if (this.isIteratorInitialized()) { //make sure the iterator is closed before changing our bounds
                this.resetIterator(false);
            }

            //split iterator must cover a prefix of the elements
            RocksColumnSpliterator split = new RocksColumnSpliterator(this, lo, mid, afterMid);
            this.children.add(split);

            this.smallestKeyInclusive = afterMid;
            //this.largestKeyInclusive = hi;
            //this.largestKeyExclusive = this.largestKeyExclusive;

            return split;
        }
    }

    @Override
    public long estimateSize() {
        if (this.reachedEnd) {
            return 0L;
        } else if (this.isIteratorInitialized()) {
            return this.intervalTree.getTotalWeightInInclusiveRange(this.iterator.key(), this.largestKeyInclusive).orElse(0L);
        } else {
            return this.intervalTree.getTotalWeightInInclusiveRange(this.smallestKeyInclusive, this.largestKeyInclusive).orElse(0L);
        }
    }

    @Override
    public int characteristics() {
        return DISTINCT | ORDERED | SORTED | NONNULL;
    }

    //TODO
    /*@Override
    public void forEachRemaining(@NonNull Consumer<? super NativeRocksHelper.KeyValueSlice> action) {
        Spliterator.super.forEachRemaining(action);
    }*/

    //TODO
    /*@Override
    public long getExactSizeIfKnown() {
        return Spliterator.super.getExactSizeIfKnown();
    }*/

    @Override
    public Comparator<? super NativeRocksHelper.KeyValueSlice> getComparator() {
        return this.sliceKeyComparator;
    }

    @Override
    public void close() throws RocksDBException {
        if (this.root == this) { //the root spliterator is the only one which actually owns any resources
            try (Options options = this.options;
                 ReadOptions readOptions = this.readOptions;
                 Snapshot snapshot = this.rootOwnsSnapshot ? this.snapshot : null) {
                //try-with-resources to ensure cleanup
            }
        }
    }

    /**
     * @author DaPorkchop_
     */
    @RequiredArgsConstructor
    protected final class WrappedSstFile implements IInclusiveRange<byte[]>, Comparable<WrappedSstFile> {
        protected final SstFileMetaData fileMeta;
        protected final int priority;

        public String fileName() {
            return this.fileMeta.fileName();
        }

        @Override
        public byte[] smallestKey() {
            return this.fileMeta.smallestKey();
        }

        @Override
        public byte[] largestKey() {
            return this.fileMeta.largestKey();
        }

        @Override
        public long weight() {
            return this.fileMeta.size();
        }

        @Override
        public int compareTo(WrappedSstFile o) {
            return Integer.compare(this.priority, o.priority);
        }
    }

    /**
     * @author DaPorkchop_
     */
    protected static class BoundedReadOptions implements AutoCloseable {
        protected final byte[] lowerBoundInclusive;
        protected final byte[] upperBoundExclusive;

        protected final Slice lowerBoundInclusiveSlice;
        protected final Slice upperBoundExclusiveSlice;
        protected final ReadOptions readOptions;

        public BoundedReadOptions(@NonNull ReadOptions readOptions, byte[] lowerBoundInclusive, byte[] upperBoundExclusive) {
            this.lowerBoundInclusive = lowerBoundInclusive;
            this.upperBoundExclusive = upperBoundExclusive;

            this.lowerBoundInclusiveSlice = lowerBoundInclusive != null ? new Slice(lowerBoundInclusive) : null;
            this.upperBoundExclusiveSlice = upperBoundExclusive != null ? new Slice(upperBoundExclusive) : null;

            this.readOptions = new ReadOptions(readOptions)
                    .setIterateLowerBound(this.lowerBoundInclusiveSlice)
                    .setIterateUpperBound(this.upperBoundExclusiveSlice);
        }

        @Override
        public void close() {
            try (Slice lowerBoundInclusiveSlice = this.lowerBoundInclusiveSlice;
                 Slice upperBoundExclusiveSlice = this.upperBoundExclusiveSlice;
                 ReadOptions readOptions = this.readOptions) {
            }
        }
    }

    /**
     * @author DaPorkchop_
     */
    protected interface IInclusiveRange<K> {
        K smallestKey();

        K largestKey();

        default long weight() {
            return 1L;
        }

        default boolean intersectsKey(@NonNull Comparator<? super K> comparator, @NonNull K key) {
            return comparator.compare(this.smallestKey(), key) <= 0
                   && comparator.compare(key, this.largestKey()) <= 0;
        }

        default boolean intersectsInclusiveRange(@NonNull Comparator<? super K> comparator, @NonNull K smallestKey, @NonNull K largestKey) {
            return comparator.compare(this.smallestKey(), largestKey) <= 0
                   && comparator.compare(this.largestKey(), smallestKey) >= 0;
        }

        default boolean containsInclusiveRange(@NonNull Comparator<? super K> comparator, @NonNull K smallestKey, @NonNull K largestKey) {
            return comparator.compare(this.smallestKey(), smallestKey) <= 0
                   && comparator.compare(largestKey, this.largestKey()) <= 0;
        }

        default boolean containedByInclusiveRange(@NonNull Comparator<? super K> comparator, @NonNull K smallestKey, @NonNull K largestKey) {
            return comparator.compare(smallestKey, this.smallestKey()) <= 0
                   && comparator.compare(this.largestKey(), largestKey) <= 0;
        }
    }

    /**
     * @author DaPorkchop_
     */
    @ThreadSafe
    protected static final class IntervalTree<K, R extends IInclusiveRange<K>> {
        private final Comparator<K> comparator;
        private final KeyOperations<K> keyOperations;
        private final Node root;

        public IntervalTree(@NonNull Comparator<K> comparator, @NonNull KeyOperations<K> keyOperations, @NonNull List<R> ranges) {
            this.comparator = comparator;
            this.keyOperations = keyOperations;

            this.root = ranges.isEmpty() ? null : new Node(ranges);
        }

        private K split(@NonNull List<R> ranges) {
            checkArg(!ranges.isEmpty());

            //get a list of all the keys
            List<K> keys = new ArrayList<>(ranges.size() * 2);
            ranges.forEach(range -> {
                keys.add(range.smallestKey());
                keys.add(range.largestKey());
            });
            keys.sort(this.comparator);

            //get the middle-est key
            if (keys.size() == 2) {
                return this.keyOperations.lerp(keys.get(0), keys.get(1), 0.5d);
            } else {
                return this.keyOperations.lerp(keys.get(keys.size() / 2), keys.get(keys.size() / 2 + 1), 0.5d);
            }
        }

        public void forEachContaining(@NonNull K key, @NonNull Consumer<? super R> action) {
            if (this.root != null) {
                this.root.forEachContaining(key, action);
            }
        }

        public List<R> getAllContaining(@NonNull K key) {
            List<R> out = new ArrayList<>();
            this.forEachContaining(key, out::add);
            return out;
        }

        public void forEachIntersectingInclusiveRange(@NonNull K smallestKey, @NonNull K largestKey, @NonNull Consumer<? super R> action) {
            if (this.root != null) {
                this.root.forEachIntersectingInclusiveRange(smallestKey, largestKey, action);
            }
        }

        public OptionalLong getTotalWeightInInclusiveRange(@NonNull K smallestKey, @NonNull K largestKey) {
            class State implements Consumer<R> {
                long sum = 0L;
                boolean any = false;

                @Override
                public void accept(R range) {
                    long weight = range.weight();
                    long addWeight;
                    if (range.containedByInclusiveRange(IntervalTree.this.comparator, smallestKey, largestKey)) {
                        //the queried range contains the entire discovered range, add its entire weight
                        addWeight = weight;
                    } else if (range.containsInclusiveRange(IntervalTree.this.comparator, smallestKey, largestKey)) {
                        //the discovered range extends beyond the queried range on both sides
                        addWeight = floorL(weight * IntervalTree.this.keyOperations.scaledDistance(smallestKey, largestKey, range.smallestKey(), range.largestKey()));
                    } else if (IntervalTree.this.comparator.compare(range.smallestKey(), smallestKey) < 0) {
                        //the discovered range extends beyond the queried range on the low end, but its upper bound is contained by the queried range
                        addWeight = floorL(weight
                                           * IntervalTree.this.keyOperations.scaledDistance(smallestKey, range.largestKey(), range.smallestKey(), range.largestKey()));
                    } else if (IntervalTree.this.comparator.compare(largestKey, range.largestKey()) < 0) {
                        //the discovered range extends beyond the queried range on the high end, but its lower bound is contained by the queried range
                        addWeight = floorL(weight
                                           * IntervalTree.this.keyOperations.scaledDistance(range.smallestKey(), largestKey, range.smallestKey(), range.largestKey()));
                    } else {
                        throw new IllegalStateException("range doesn't intersect the queried range?!?");
                    }

                    this.sum = Math.addExact(this.sum, addWeight);
                    this.any = true;
                }
            }

            State state = new State();
            this.forEachIntersectingInclusiveRange(smallestKey, largestKey, state);
            return state.any ? OptionalLong.of(state.sum) : OptionalLong.empty();
        }

        /**
         * @author DaPorkchop_
         */
        protected final class Node implements IInclusiveRange<K> {
            @Getter
            private final K smallestKey;
            @Getter
            private final K largestKey;

            private final List<R> values;

            private final K centerKey;
            private final Node left;
            private final Node right;

            public Node(@NonNull List<R> ranges) {
                this.smallestKey = ranges.stream().map(R::smallestKey).min(IntervalTree.this.comparator).get();
                this.largestKey = ranges.stream().map(R::largestKey).max(IntervalTree.this.comparator).get();

                if (ranges.size() <= 8) {
                    this.values = new ArrayList<>(ranges);
                    this.centerKey = null;
                    this.left = null;
                    this.right = null;
                } else {
                    this.centerKey = IntervalTree.this.split(ranges);

                    List<R> leftValues = new ArrayList<>();
                    List<R> rightValues = new ArrayList<>();
                    List<R> intersectingValues = new ArrayList<>();

                    ranges.forEach(range -> {
                        if (IntervalTree.this.comparator.compare(range.largestKey(), this.centerKey) < 0) {
                            leftValues.add(range);
                        } else if (IntervalTree.this.comparator.compare(this.centerKey, range.smallestKey()) < 0) {
                            rightValues.add(range);
                        } else {
                            intersectingValues.add(range);
                        }
                    });

                    this.values = intersectingValues.isEmpty() ? Collections.emptyList() : intersectingValues;
                    this.left = leftValues.isEmpty() ? null : new Node(leftValues);
                    this.right = rightValues.isEmpty() ? null : new Node(rightValues);
                }
            }

            public void forEachContaining(@NonNull K key, @NonNull Consumer<? super R> action) {
                if (this.intersectsKey(IntervalTree.this.comparator, key)) {
                    if (this.left != null) {
                        this.left.forEachContaining(key, action);
                    }

                    this.values.forEach(range -> {
                        if (range.intersectsKey(IntervalTree.this.comparator, key)) {
                            action.accept(range);
                        }
                    });

                    if (this.right != null) {
                        this.right.forEachContaining(key, action);
                    }
                }
            }

            public void forEachIntersectingInclusiveRange(@NonNull K smallestKey, @NonNull K largestKey, @NonNull Consumer<? super R> action) {
                if (this.intersectsInclusiveRange(IntervalTree.this.comparator, smallestKey, largestKey)) {
                    if (this.left != null) {
                        this.left.forEachIntersectingInclusiveRange(smallestKey, largestKey, action);
                    }

                    this.values.forEach(range -> {
                        if (range.intersectsInclusiveRange(IntervalTree.this.comparator, smallestKey, largestKey)) {
                            action.accept(range);
                        }
                    });

                    if (this.right != null) {
                        this.right.forEachIntersectingInclusiveRange(smallestKey, largestKey, action);
                    }
                }
            }
        }
    }

    /**
     * @author DaPorkchop_
     */
    public interface KeyOperations<K> {
        KeyOperations<byte[]> FIXED_SIZE_LEX_ORDER = new KeyOperations<byte[]>() {
            @Override
            public Optional<byte[]> increment(@NonNull byte[] key) {
                key = key.clone();

                for (int i = key.length - 1; i >= 0; i--) {
                    key[i]++;
                    if (key[i] != 0) { //no overflow
                        return Optional.of(key);
                    }
                }

                //integer would overflow
                return Optional.empty();
            }

            private BigInteger toBigInteger(byte[] arr) {
                //add a single zero byte as the MSB so that it's unsigned
                byte[] clone = new byte[arr.length + 1];
                System.arraycopy(arr, 0, clone, 1, arr.length);
                return new BigInteger(clone);
            }

            @Override
            public byte[] lerp(@NonNull byte[] a, @NonNull byte[] b, double t) {
                int n = a.length;
                checkArg(b.length == n);

                if (Arrays.equals(a, b) || t <= 0.0d) {
                    return a.clone();
                } else if (t >= 1.0d) {
                    return b.clone();
                }

                //re-interpret as big-endian unsigned integers, then convert to BigDecimal
                BigDecimal aDecimal = new BigDecimal(this.toBigInteger(a));
                BigDecimal bDecimal = new BigDecimal(this.toBigInteger(b));

                //result = a + (b - a) * t
                BigDecimal resultDecimal = aDecimal.add(BigDecimal.valueOf(t).multiply(bDecimal.subtract(aDecimal)));
                BigInteger resultInteger = resultDecimal.toBigInteger();

                byte[] result = resultInteger.toByteArray();
                if (result.length != n) { //sign-extend to exactly n bytes
                    checkState(result.length < n);

                    byte[] extendedResult = new byte[n];
                    System.arraycopy(result, 0, extendedResult, n - result.length, result.length);

                    //sanity check
                    checkState(new BigInteger(extendedResult).equals(resultInteger));

                    return extendedResult;
                }

                return result;
            }

            @Override
            public double scaledDistance(@NonNull byte[] dist0, @NonNull byte[] dist1, @NonNull byte[] scale0, @NonNull byte[] scale1) {
                int n = dist0.length;
                checkArg(dist1.length == n && scale0.length == n && scale1.length == n);

                //re-interpret as big-endian unsigned integers, then convert to BigDecimal
                BigDecimal dist0Decimal = new BigDecimal(this.toBigInteger(dist0));
                BigDecimal dist1Decimal = new BigDecimal(this.toBigInteger(dist1));
                BigDecimal scale0Decimal = new BigDecimal(this.toBigInteger(scale0));
                BigDecimal scale1Decimal = new BigDecimal(this.toBigInteger(scale1));

                //(dist1 - dist0) / (scale1 - scale0)
                return (dist1Decimal.subtract(dist0Decimal)).divide(scale1Decimal.subtract(scale0Decimal), MathContext.DECIMAL64).doubleValue();
            }
        };

        Optional<K> increment(@NonNull K key);

        /**
         * Returns a key approximately equivalent to {@code a + (b - a) * t}.
         *
         * @param a the first key
         * @param b the second key
         * @param t the blending factor. Must be in range {@code [0, 1]}.
         * @return a key approximately equivalent to {@code a + (b - a) * t}
         */
        K lerp(@NonNull K a, @NonNull K b, double t);

        /**
         * Returns a {@code double} approximately equivalent to the distance between {@code dist0} and {@code dist1}, scaled by the distance between {@code scale0} and
         * {@code scale1}, i.e. {@code (dist1 - dist0) / (scale1 - scale0)}.
         *
         * @param dist0  the first key for computing the distance
         * @param dist1  the second key for computing the distance
         * @param scale0 the first key for computing the scale factor
         * @param scale1 the second key for computing the scale factor
         * @return a {@code double} approximately equivalent to {@code (dist1 - dist0) / (scale1 - scale0)}
         */
        double scaledDistance(@NonNull K dist0, @NonNull K dist1, @NonNull K scale0, @NonNull K scale1);
    }
}
