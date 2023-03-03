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

package net.daporkchop.tpposmtilegen.storage.map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import net.daporkchop.lib.primitive.lambda.LongLongConsumer;
import net.daporkchop.lib.primitive.lambda.LongLongObjConsumer;
import net.daporkchop.lib.primitive.lambda.LongObjConsumer;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBIterator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import net.daporkchop.tpposmtilegen.util.DuplicatedList;
import net.daporkchop.tpposmtilegen.util.Threading;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyMetaData;
import org.rocksdb.Options;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileMetaData;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.common.util.PorkUtil.*;
import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public abstract class RocksDBMap<V> extends WrappedRocksDB {
    public RocksDBMap(Database database, ColumnFamilyHandle column, ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public void put(@NonNull DBWriteAccess access, long key, @NonNull V value) throws Exception {
        ByteBuffer keyBuffer = DIRECT_BUFFER_RECYCLER_8.get();
        ByteBuf buf = WRITE_BUFFER_CACHE.get();

        keyBuffer.clear();
        keyBuffer.putLong(key).flip();

        this.valueToBytes(value, buf.clear());
        access.put(this.column, keyBuffer, buf.internalNioBuffer(0, buf.readableBytes()));
    }

    public void delete(@NonNull DBWriteAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            //serialize key to bytes
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
            access.delete(this.column, keyArray);
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public V get(@NonNull DBReadAccess access, long key) throws Exception {
        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        byte[] keyArray = keyArrayRecycler.get();
        try {
            PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);

            byte[] valueData = access.get(this.column, keyArray);
            return valueData != null ? this.valueFromBytes(key, Unpooled.wrappedBuffer(valueData)) : null;
        } finally {
            keyArrayRecycler.release(keyArray);
        }
    }

    public List<V> getAll(@NonNull DBReadAccess access, @NonNull LongList keys) throws Exception {
        int size = keys.size();
        if (size == 0) {
            return Collections.emptyList();
        } else if (size > 10000) { //split into smaller gets (prevents what i can only assume is a rocksdbjni bug where it will throw an NPE when requesting too many elements at once
            List<V> dst = new ArrayList<>(size);
            for (int i = 0; i < size; i += 10000) {
                dst.addAll(this.getAll(access, keys.subList(i, min(i + 10000, size))));
            }
            return dst;
        }

        ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
        List<byte[]> keyBytes = new ArrayList<>(size);
        List<byte[]> valueBytes;
        try {
            //serialize keys to bytes
            for (int i = 0; i < size; i++) {
                byte[] keyArray = keyArrayRecycler.get();
                long key = keys.getLong(i);
                PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);
                keyBytes.add(keyArray);
            }

            //look up values from key
            valueBytes = access.multiGetAsList(new DuplicatedList<>(this.column, size), keyBytes);
        } finally {
            keyBytes.forEach(keyArrayRecycler::release);
        }

        //re-use list that was previously used for storing encoded keys and store deserialized values in it
        keyBytes.clear();
        List<V> values = uncheckedCast(keyBytes);

        for (int i = 0; i < size; i++) {
            byte[] value = valueBytes.get(i);
            values.add(value != null ? this.valueFromBytes(keys.getLong(i), Unpooled.wrappedBuffer(value)) : null);
        }
        return values;
    }

    public void forEach(@NonNull DBReadAccess access, @NonNull LongObjConsumer<? super V> callback) throws Exception {
        try (DBIterator itr = access.iterator(this.column)) {
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                long key = PUnsafe.getUnalignedLongBE(itr.key(), PUnsafe.arrayByteElementOffset(0));
                callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(itr.value())));
            }
        }
    }

    public void forEachParallel(@NonNull DBReadAccess access, @NonNull LongObjConsumer<? super V> callback) throws Exception {
        this.forEachParallel(access, callback, (firstKey, lastKey) -> {}, (firstKey, lastKey, t) -> {});
    }

    public void forEachParallel(@NonNull DBReadAccess access, @NonNull LongObjConsumer<? super V> callback,
                                @NonNull LongLongConsumer beginThreadLocalSortedCallback,
                                @NonNull LongLongObjConsumer<? super Throwable> endThreadLocalSortedBlockCallback) throws Exception {
        if (access.isDirectRead()) {
            Optional<Snapshot> optionalInternalSnapshot = access.internalSnapshot();
            try (Snapshot temporarySnapshotToCloseLater = optionalInternalSnapshot.isPresent() ? null : this.database.delegate().getSnapshot();
                 Options options = new Options(this.database.config().dbOptions(), this.database.columns().get(this.column).getOptions())) {
                Snapshot snapshot = optionalInternalSnapshot.orElse(temporarySnapshotToCloseLater);
                long sequenceNumber = snapshot.getSequenceNumber();

                ColumnFamilyMetaData columnMeta = this.database.delegate().getColumnFamilyMetaData(this.column);
                switch (toInt(columnMeta.levels().stream().filter(levelMeta -> levelMeta.size() != 0L).count())) {
                    case 0: //nothing to do
                        return;
                    case 1: //there's exactly one non-empty level, we can simply iterate over all the SST files in the level in parallel
                        Threading.<SstFileMetaData>iterateParallel(CPU_COUNT,
                                c -> columnMeta.levels().stream().filter(levelMeta -> levelMeta.size() != 0L).findAny().get().files().stream().forEach(c),
                                fileMeta -> {
                                    if (fileMeta.smallestSeqno() > sequenceNumber) { //the file is newer than this snapshot
                                        //TODO: we could improve the detection for this by a lot
                                        return;
                                    }

                                    long firstKey = PUnsafe.getUnalignedLongBE(fileMeta.smallestKey(), PUnsafe.arrayByteElementOffset(0));
                                    long lastKey = PUnsafe.getUnalignedLongBE(fileMeta.largestKey(), PUnsafe.arrayByteElementOffset(0));
                                    beginThreadLocalSortedCallback.accept(firstKey, lastKey);

                                    try (SstFileReader reader = new SstFileReader(options)) {
                                        reader.open(fileMeta.path() + fileMeta.fileName());
                                        try (SstFileReaderIterator iterator = reader.newIterator(this.database.config().readOptions(DatabaseConfig.ReadType.BULK_ITERATE))) {
                                            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                                                long key = PUnsafe.getUnalignedLongBE(iterator.key(), PUnsafe.arrayByteElementOffset(0));
                                                callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(iterator.value())));
                                            }
                                        }
                                    } catch (Throwable t) {
                                        try {
                                            endThreadLocalSortedBlockCallback.accept(firstKey, lastKey, t);
                                        } catch (Throwable t1) {
                                            t.addSuppressed(t1);
                                        }
                                        throw PUnsafe.throwException(t);
                                    }
                                    endThreadLocalSortedBlockCallback.accept(firstKey, lastKey, null);
                                });
                        return;
                }
            }

            logger.warn("column family '%s' isn't compacted, can't use fast parallel iteration! consider compacting the db first.",
                    new String(this.column.getName(), StandardCharsets.UTF_8));
        }

        @AllArgsConstructor
        class ValueWithKey {
            final byte[] key;
            final byte[] value;
        }

        Threading.<ValueWithKey>iterateParallel(32 * CPU_COUNT,
                c -> {
                    try (DBIterator itr = access.iterator(this.column)) {
                        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                            c.accept(new ValueWithKey(itr.key(), itr.value()));
                        }
                    }
                },
                v -> {
                    long key = PUnsafe.getUnalignedLongBE(v.key, PUnsafe.arrayByteElementOffset(0));
                    callback.accept(key, this.valueFromBytes(key, Unpooled.wrappedBuffer(v.value)));
                });
    }

    public void forEachKeyParallel(@NonNull DBReadAccess access, @NonNull LongConsumer callback, @NonNull KeyDistribution distribution) throws Exception {
        switch (distribution) {
            case UNKNOWN:
            case SCATTERED:
            case EVEN_SPARSE:
                Threading.<byte[]>iterateParallel(32 * CPU_COUNT,
                        c -> {
                            try (DBIterator itr = access.iterator(this.column)) {
                                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                                    c.accept(itr.key());
                                }
                            }
                        },
                        keyArray -> {
                            long key = PUnsafe.getUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0));
                            callback.accept(key);
                        });
                break;
            case EVEN_DENSE: {
                long firstKey;
                long lastKey;
                try (DBIterator itr = access.iterator(this.column)) {
                    itr.seekToFirst();
                    if (!itr.isValid()) { //column family is empty
                        return;
                    }
                    firstKey = PUnsafe.getUnalignedLongBE(itr.key(), PUnsafe.arrayByteElementOffset(0));
                    itr.seekToLast();
                    checkState(itr.isValid());
                    lastKey = PUnsafe.getUnalignedLongBE(itr.key(), PUnsafe.arrayByteElementOffset(0));
                }

                //TODO: this could be improved further by using an iterator for sub-ranges
                Threading.forEachParallelLong(key -> {
                    ByteArrayRecycler keyArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
                    byte[] keyArray = keyArrayRecycler.get();
                    try {
                        PUnsafe.putUnalignedLongBE(keyArray, PUnsafe.arrayByteElementOffset(0), key);

                        if (access.get(this.column, keyArray) != null) {
                            callback.accept(key);
                        }
                    } catch (Exception e) {
                        PUnsafe.throwException(e);
                    } finally {
                        keyArrayRecycler.release(keyArray);
                    }
                }, LongStream.rangeClosed(firstKey, lastKey).spliterator());
                break;
            }
            default:
                throw new IllegalArgumentException(distribution.name());
        }
    }

    protected abstract void valueToBytes(@NonNull V value, @NonNull ByteBuf dst);

    protected abstract V valueFromBytes(long key, @NonNull ByteBuf valueBytes);

    public enum KeyDistribution {
        UNKNOWN,
        SCATTERED,
        EVEN_SPARSE,
        EVEN_DENSE,
    }
}
