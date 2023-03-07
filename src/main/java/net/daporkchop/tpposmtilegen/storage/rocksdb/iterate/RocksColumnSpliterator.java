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
import net.daporkchop.lib.common.annotation.NotThreadSafe;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    protected final List<WrappedSstFile> bottommostNonzeroLevelFiles;
    //protected final List<List<SstFileMetaData>> bottommostFilesGroupedByOverlaps;

    protected final List<WrappedSstFile> wrappedSstFiles;
    protected final List<IterationSegment> segments;

    protected IterationSegment currentSegment;
    protected int nextSegmentIndex;
    protected int nextSegmentFence;

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
                database.delegate().flush(database.config().flushOptions(DatabaseConfig.FlushType.GENERAL), column);
                database.delegate().flushWal(false);
            }
            this.snapshot = database.delegate().getSnapshot();
        }

        this.options = new Options(database.config().dbOptions(), database.columns().get(column).getOptions());
        this.readOptions = new ReadOptions(database.config().readOptions(readType)).setSnapshot(this.snapshot);

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

        /*this.bottommostNonzeroLevelFiles = IntStream.range(0, this.sortedNonzeroLevelFiles.size())
                .mapToObj(levelIndex -> this.sortedNonzeroLevelFiles.get(levelIndex).stream()
                        .filter(fileMeta -> this.sortedNonzeroLevelFiles.subList(0, levelIndex).stream().flatMap(List::stream)
                                .noneMatch(otherFileMeta -> this.intersects(fileMeta, otherFileMeta))))
                .flatMap(Function.identity())
                .sorted(Comparator.comparing(SstFileMetaData::smallestKey, this.heapKeyComparator))
                .collect(Collectors.toList());*/

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

        this.bottommostNonzeroLevelFiles = this.wrappedSstFiles.stream()
                .filter(file -> this.wrappedSstFiles.stream()
                        .noneMatch(otherFile -> file.priority > otherFile.priority && this.intersects(file.fileMeta, otherFile.fileMeta)))
                .collect(Collectors.toList());

        Index<byte[], WrappedSstFile> index = new Index<>(this.heapKeyComparator, keyOperations);
        this.wrappedSstFiles.forEach(index::insert);
        this.segments = index.map.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> new IterationSegment(
                        entry.getValue().stream().sorted().map(file -> file.fileMeta).collect(Collectors.toList()),
                        entry.getKey(), index.map.higherKey(entry.getKey())))
                .collect(Collectors.toList());

        this.currentSegment = null;
        this.nextSegmentIndex = 0;
        this.nextSegmentFence = this.segments.size();
    }

    private RocksColumnSpliterator(@NonNull RocksColumnSpliterator parent, int nextSegmentIndex, int nextSegmentFence) {
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
        this.bottommostNonzeroLevelFiles = parent.bottommostNonzeroLevelFiles;

        this.wrappedSstFiles = parent.wrappedSstFiles;
        this.segments = parent.segments;

        this.currentSegment = null;
        this.nextSegmentIndex = nextSegmentIndex;
        this.nextSegmentFence = nextSegmentFence;
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

    protected boolean intersects(@NonNull SstFileMetaData a, @NonNull SstFileMetaData b) {
        return this.heapKeyComparator.compare(a.smallestKey(), b.largestKey()) <= 0
               && this.heapKeyComparator.compare(a.largestKey(), b.smallestKey()) >= 0;
    }

    @Override
    public boolean tryAdvance(@NonNull Consumer<? super NativeRocksHelper.KeyValueSlice> action) {
        while (true) {
            if (this.currentSegment == null) { //there is no current segment, we need to start a new one
                if (this.nextSegmentIndex < this.nextSegmentFence) { //there is another segment available to take from
                    this.currentSegment = this.segments.get(this.nextSegmentIndex++);
                    this.currentSegment.start();
                } else { //there are no more segments to be returned
                    return false;
                }
            }

            NativeRocksHelper.KeyValueSlice currentKeyValueSlice = this.currentSegment.nextOrNull();
            if (currentKeyValueSlice != null) { //a value was found, return it as usual
                action.accept(currentKeyValueSlice);
                return true;
            } else { //we've reached this segment's end!
                this.currentSegment.finish();
                this.currentSegment = null;
            }
        }
    }

    @Override
    public RocksColumnSpliterator trySplit() {
        //TODO: would be cool to support splitting within an individual segment
        int lo = this.nextSegmentIndex;
        int hi = this.nextSegmentFence;
        int mid = (lo + hi) >>> 1;
        if (lo >= mid) {
            return null;
        } else {
            RocksColumnSpliterator split = new RocksColumnSpliterator(this, lo, this.nextSegmentIndex = mid);
            this.children.add(split);
            return split;
        }
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
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
        public int compareTo(WrappedSstFile o) {
            return Integer.compare(this.priority, o.priority);
        }
    }

    /**
     * @author DaPorkchop_
     */
    @RequiredArgsConstructor
    protected class IterationSegment {
        @NonNull
        protected final List<SstFileMetaData> files;

        protected final byte[] smallestKeyInclusive;
        protected final byte[] largestKeyExclusive;

        private boolean started = false;
        private Slice lowerBoundSlice;
        private Slice upperBoundSlice;
        private ReadOptions readOptions;

        private RocksIterator iterator;
        private NativeRocksHelper.KeyValueSlice currentKeyValueSlice;
        private boolean reachedEnd = false;

        public synchronized void start() {
            checkState(!this.started, "already started?!?");
            this.started = true;

            this.lowerBoundSlice = new Slice(this.smallestKeyInclusive);
            this.upperBoundSlice = new Slice(this.largestKeyExclusive);
            this.readOptions = new ReadOptions(RocksColumnSpliterator.this.readOptions)
                    .setIterateLowerBound(this.lowerBoundSlice)
                    .setIterateUpperBound(this.upperBoundSlice);

            this.iterator = RocksColumnSpliterator.this.database.delegate().newIterator(RocksColumnSpliterator.this.column, this.readOptions);
        }

        public NativeRocksHelper.KeyValueSlice nextOrNull() {
            if (this.currentKeyValueSlice == null) { //first time
                this.currentKeyValueSlice = new NativeRocksHelper.KeyValueSlice();
                this.iterator.seekToFirst();
            } else { //advance
                checkState(!this.reachedEnd, "trying to advance beyond end?!?");
                this.iterator.next();
            }

            if (this.iterator.isValid()) {
                NativeRocksHelper.getKeyAndValueAsView(this.iterator, this.currentKeyValueSlice);
                return this.currentKeyValueSlice;
            } else {
                this.reachedEnd = true;
                return null;
            }
        }

        public synchronized void finish() {
            checkState(this.started, "not started?!?");

            //close everything
            try (RocksIterator iterator = this.iterator;
                 ReadOptions readOptions = this.readOptions;
                 Slice upperBoundSlice = this.upperBoundSlice;
                 Slice lowerBoundSlice = this.lowerBoundSlice) {
                this.lowerBoundSlice = null;
                this.upperBoundSlice = null;
                this.readOptions = null;
                this.iterator = null;
                this.currentKeyValueSlice = null;
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
            return comparator.compare(this.smallestKey(), key) <= 0 && comparator.compare(key, this.largestKey()) <= 0;
        }
    }

    /**
     * @author DaPorkchop_
     */
    @NotThreadSafe
    protected static final class Index<K, R extends IInclusiveRange<K>> {
        private final Comparator<K> comparator;
        private final KeyOperations<K> keyOperations;
        private final NavigableMap<K, List<R>> map;

        public Index(@NonNull Comparator<K> comparator, @NonNull KeyOperations<K> keyOperations) {
            this.comparator = comparator;
            this.keyOperations = keyOperations;
            this.map = new TreeMap<>(comparator);
        }

        public void insert(@NonNull R range) {
            checkState(this.comparator.compare(range.smallestKey(), range.largestKey()) != 0, "don't know what to do with a range of length 0!");

            {
                Map.Entry<K, List<R>> entry = this.map.floorEntry(range.smallestKey());
                if (entry == null) { //new smallest key!
                    this.map.put(range.smallestKey(), new ArrayList<>());
                } else if (this.comparator.compare(range.smallestKey(), entry.getKey()) != 0) { //the key isn't already present, insert a new segment
                    this.map.put(range.smallestKey(), new ArrayList<>(entry.getValue()));
                }
            }

            {
                K onePlusLargestKey = this.keyOperations.increment(range.largestKey()).get();
                Map.Entry<K, List<R>> entry = this.map.floorEntry(onePlusLargestKey);
                checkState(entry != null); //there is always a smaller key
                if (this.comparator.compare(onePlusLargestKey, entry.getKey()) != 0) {
                    //the upper bound key isn't already present, insert a new segment
                    this.map.put(onePlusLargestKey, new ArrayList<>(entry.getValue()));
                }
            }

            for (Map.Entry<K, List<R>> entry : this.map.subMap(range.smallestKey(), true, range.largestKey(), true).entrySet()) {
                entry.getValue().add(range);
            }

            checkState(this.map.lastEntry().getValue().isEmpty());
        }
    }

    /**
     * @author DaPorkchop_
     */
    @NotThreadSafe
    protected static final class IntervalTree<K, R extends IInclusiveRange<K>> {
        private final Comparator<K> comparator;
        private final Node root;

        public IntervalTree(@NonNull Comparator<K> comparator, @NonNull List<R> ranges) {
            this.comparator = comparator;

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
            K key = keys.get(keys.size() >> 1);
            keys.clear();
            return key;
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

        /**
         * @author DaPorkchop_
         */
        protected final class Node implements IInclusiveRange<K> {
            @Getter
            private final K smallestKey;
            @Getter
            private final K largestKey;

            private final K centerKey;

            private final List<R> values;

            private final Node left;
            private final Node right;

            public Node(@NonNull List<R> ranges) {
                this.centerKey = IntervalTree.this.split(ranges);

                this.smallestKey = ranges.stream().map(R::smallestKey).min(IntervalTree.this.comparator).get();
                this.largestKey = ranges.stream().map(R::largestKey).max(IntervalTree.this.comparator).get();

                if (ranges.size() <= 8) {
                    this.values = new ArrayList<>(ranges);
                    this.left = null;
                    this.right = null;
                } else {
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
                if (!this.intersectsKey(IntervalTree.this.comparator, key)) {
                    return;
                }

                this.values.forEach(range -> {
                    if (range.intersectsKey(IntervalTree.this.comparator, key)) {
                        action.accept(range);
                    }
                });

                if (this.left != null) {
                    this.left.forEachContaining(key, action);
                }
                if (this.right != null) {
                    this.right.forEachContaining(key, action);
                }
            }
        }
    }

    /*@NotThreadSafe
    protected static final class Index<K, R extends IInclusiveRange<K>> {
        private final Comparator<K> comparator;
        private final NavigableMap<K, List<EdgeMarker<R>>> map;

        public Index(@NonNull Comparator<K> comparator) {
            this.comparator = comparator;
            this.map = new TreeMap<>(comparator);
        }

        public void insert(@NonNull R range) {
            checkState(this.comparator.compare(range.smallestKey(), range.largestKey()) != 0, "don't know what to do with a range of length 0!");
            this.map.computeIfAbsent(range.smallestKey(), k -> new ArrayList<>()).add(new EdgeMarker<>(range, false));
            this.map.computeIfAbsent(range.largestKey(), k -> new ArrayList<>()).add(new EdgeMarker<>(range, true));
        }

        public List<Segment<K, R>> segments() {
            List<Segment<K, R>> segments = new ArrayList<>();
            Set<R> visibleValues = new HashSet<>();

            K lastKey = null;
            for (Map.Entry<K, List<EdgeMarker<R>>> entry : this.map.entrySet()) {
                K currKey = entry.getKey();

                entry.getValue().forEach(marker -> {
                    if (!marker.end) {
                        checkState(visibleValues.add(marker.value));
                    }
                });
                segments.add(new Segment<>(new ArrayList<>(visibleValues), PorkUtil.fallbackIfNull(lastKey, currKey), currKey));
                entry.getValue().forEach(marker -> {
                    if (marker.end) {
                        checkState(visibleValues.remove(marker.value));
                    }
                });

                lastKey = currKey;
            }
            checkState(visibleValues.isEmpty());

            return segments;
        }

        @RequiredArgsConstructor
        @EqualsAndHashCode
        private static final class EdgeMarker<V> {
            @NonNull
            private final V value;
            private final boolean end;
        }

        @RequiredArgsConstructor
        @EqualsAndHashCode
        @Getter
        private static final class Segment<K, V> implements IInclusiveRange<K> {
            @NonNull
            private final List<V> values;
            @NonNull
            private final K smallestKey;
            @NonNull
            private final K largestKey;
        }
    }*/

    /**
     * @author DaPorkchop_
     */
    @FunctionalInterface
    public interface KeyOperations<K> {
        KeyOperations<byte[]> FIXED_SIZE_LEX_ORDER = key -> {
            key = key.clone();

            for (int i = key.length - 1; i >= 0; i--) {
                key[i]++;
                if (key[i] != 0) { //no overflow
                    return Optional.of(key);
                }
            }

            //integer would overflow
            return Optional.empty();
        };

        Optional<K> increment(@NonNull K key);
    }
}
