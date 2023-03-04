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

package natives;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import net.daporkchop.tpposmtilegen.natives.UInt64ToBlobMapMergeOperator;
import net.daporkchop.tpposmtilegen.storage.rocksdb.DatabaseConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksMemEnv;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.daporkchop.tpposmtilegen.natives.UInt64ToBlobMapMergeOperator.*;
import static org.junit.Assert.*;

/**
 * @author DaPorkchop_
 */
public class TestUInt64ToBlobMapMergeOperator {
    private static final DatabaseConfig DATABASE_CONFIG = DatabaseConfig.RW_GENERAL;

    private UInt64ToBlobMapMergeOperator mergeOperator;

    private Env env;
    private Options options;
    private FlushOptions flushOptions;
    private WriteOptions writeOptions;
    private RocksDB db;

    @Before
    public void create() throws Exception {
        this.mergeOperator = new UInt64ToBlobMapMergeOperator();

        this.env = new RocksMemEnv(DATABASE_CONFIG.dbOptions().getEnv());
        this.options = new Options(DATABASE_CONFIG.dbOptions(), DATABASE_CONFIG.columnFamilyOptions(DatabaseConfig.ColumnFamilyType.FAST))
                .setCreateIfMissing(true)
                .setEnv(this.env)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false)
                .setMergeOperator(this.mergeOperator)
                .setMaxSuccessiveMerges(1);

        this.flushOptions = new FlushOptions();
        this.writeOptions = new WriteOptions();

        this.db = RocksDB.open(this.options, "/jeff");
    }

    @After
    public void destroy() throws Exception {
        try (UInt64ToBlobMapMergeOperator mergeOperator = this.mergeOperator;
             Env env = this.env;
             Options options = this.options;
             FlushOptions flushOptions = this.flushOptions;
             WriteOptions writeOptions = this.writeOptions;
             RocksDB db = this.db) {
            this.mergeOperator = null;
            this.env = null;
            this.options = null;
            this.flushOptions = null;
            this.writeOptions = null;
            this.db = null;
        }
    }

    private static Long2ObjectSortedMap<String> buildMap(Object... args) {
        Long2ObjectSortedMap<String> map = new Long2ObjectRBTreeMap<>();
        for (int i = 0; i < args.length; ) {
            map.put(((Long) args[i++]).longValue(), (String) args[i++]);
        }
        return map;
    }

    private static byte[] concat(byte[]... arrs) {
        byte[] dst = new byte[Stream.of(arrs).mapToInt(arr -> arr.length).sum()];
        ByteBuf buf = Unpooled.wrappedBuffer(dst).clear();
        for (byte[] arr : arrs) {
            buf.writeBytes(arr);
        }
        return dst;
    }

    @Test
    public void testSimple() throws RocksDBException {
        this.db.put("a".getBytes(), add(5, "hello"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(5L, "hello"));
        this.db.flush(this.flushOptions);

        this.db.merge("a".getBytes(), add(7, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(5L, "hello", 7L, "world"));

        this.db.merge("a".getBytes(), del(5));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(7L, "world"));

        this.db.compactRange();

        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(7L, "world"));
    }

    @Test
    public void testComplexPartialMerge() throws RocksDBException {
        this.db.put("a".getBytes(), concat(add(5, "hello"), add(6, "world"), add(7, "!")));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(5L, "hello", 6L, "world", 7L, "!"));
        this.db.flush(this.flushOptions);

        this.db.merge("a".getBytes(), concat(add(1, "name"), add(2, "jeff"), add(3, "lol"), del(4), del(5)));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(1L, "name", 2L, "jeff", 3L, "lol", 6L, "world", 7L, "!"));

        this.db.merge("a".getBytes(), concat(del(1), add(2, "jef"), del(4), add(5, "hey"), del(7)));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));

        this.db.compactRange();
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
    }

    @Test
    public void testComplexPartialMerge_InitialMerge() throws RocksDBException {
        this.db.merge("a".getBytes(), concat(add(5, "hello"), add(6, "world"), add(7, "!")));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(5L, "hello", 6L, "world", 7L, "!"));
        this.db.flush(this.flushOptions);

        this.db.merge("a".getBytes(), concat(add(1, "name"), add(2, "jeff"), add(3, "lol"), del(4), del(5)));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(1L, "name", 2L, "jeff", 3L, "lol", 6L, "world", 7L, "!"));

        this.db.merge("a".getBytes(), concat(del(1), add(2, "jef"), del(4), add(5, "hey"), del(7)));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));

        this.db.compactRange();
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
    }

    @Test
    public void testComplexPartialMerge_InitialMerge_Batch() throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            batch.merge("a".getBytes(), concat(add(5, "hello"), add(6, "world"), add(7, "!")));
            batch.merge("a".getBytes(), concat(add(1, "name"), add(2, "jeff"), add(3, "lol"), del(4), del(5)));
            batch.merge("a".getBytes(), concat(del(1), add(2, "jef"), del(4), add(5, "hey"), del(7)));
            this.db.write(this.writeOptions, batch);
        }

        this.db.compactRange();

        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
        assertEquals(decodeToStrings(this.db.get("a".getBytes())), buildMap(2L, "jef", 3L, "lol", 5L, "hey", 6L, "world"));
    }

    /*@Test
    public void testRandom() throws RocksDBException {
        String[] keys = { "a", "b", "c", "d" };
        Map<String, Long2ObjectSortedMap<String>> states = Stream.of(keys).collect(Collectors.toMap(Function.identity(), key -> new Long2ObjectRBTreeMap<>()));

        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (int i = 0; i < 100; i++) {
            String key = keys[r.nextInt(keys.length)];
            Long2ObjectSortedMap<String> state = states.get(key);

            int adds = r.nextInt(5);
            int dels = r.nextInt(3);
            final long RANGE = 10;

            LongBuffer buffer = LongBuffer.allocate(2 + adds + dels)
                    .put(adds).put(dels);

            long[] addsVals = r.longs(0L, RANGE).distinct().limit(adds).sorted().toArray();
            buffer.put(addsVals);
            for (long val : addsVals) {
                state.add(val);
            }

            long[] delsVals = r.longs(0L, RANGE).filter(val -> Arrays.binarySearch(addsVals, val) < 0).distinct().limit(dels).sorted().toArray();
            buffer.put(delsVals);
            for (long val : delsVals) {
                state.remove(val);
            }

            this.db.merge(key.getBytes(),_toBytes(buffer.array()));

            if (r.nextInt(100) == 0) {
                long[] expected = state.toLongArray();
                long[] found = decodeToStrings(PorkUtil.fallbackIfNull(this.db.get(key.getBytes()), new byte[0]));
                if (!Arrays.equals(expected, found)) {
                    throw new IllegalStateException(Arrays.toString(expected) + " != " + Arrays.toString(found));
                }
            }
        }

        for (String key : keys) {
            long[] expected = states.get(key).toLongArray();
            long[] found = decodeToStrings(PorkUtil.fallbackIfNull(this.db.get(key.getBytes()), new byte[0]));
            if (!Arrays.equals(expected, found)) {
                throw new IllegalStateException(Arrays.toString(expected) + " != " + Arrays.toString(found));
            }
        }

        this.db.compactRange();

        for (String key : keys) {
            long[] expected = states.get(key).toLongArray();
            long[] found = decodeToStrings(PorkUtil.fallbackIfNull(this.db.get(key.getBytes()), new byte[0]));
            if (!Arrays.equals(expected, found)) {
                throw new IllegalStateException(Arrays.toString(expected) + " != " + Arrays.toString(found));
            }
        }
    }*/
}
