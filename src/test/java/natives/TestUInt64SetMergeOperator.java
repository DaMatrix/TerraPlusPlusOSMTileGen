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

import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.tpposmtilegen.natives.UInt64SetMergeOperator;
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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * @author DaPorkchop_
 */
public class TestUInt64SetMergeOperator {
    private static final DatabaseConfig DATABASE_CONFIG = DatabaseConfig.RW_GENERAL;

    private UInt64SetMergeOperator mergeOperator;

    private Env env;
    private Options options;
    private FlushOptions flushOptions;
    private RocksDB db;

    @Before
    public void create() throws Exception {
        this.mergeOperator = new UInt64SetMergeOperator();

        this.env = new RocksMemEnv(DATABASE_CONFIG.dbOptions().getEnv());
        this.options = new Options(DATABASE_CONFIG.dbOptions(), DATABASE_CONFIG.columnFamilyOptions(DatabaseConfig.ColumnFamilyType.FAST))
                .setCreateIfMissing(true)
                .setEnv(this.env)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false)
                .setMergeOperator(this.mergeOperator);

        this.flushOptions = new FlushOptions();

        this.db = RocksDB.open(this.options, "/jeff");
    }

    @After
    public void destroy() throws Exception {
        try (UInt64SetMergeOperator mergeOperator = this.mergeOperator;
             Env env = this.env;
             Options options = this.options;
             FlushOptions flushOptions = this.flushOptions;
             RocksDB db = this.db) {
            this.mergeOperator = null;
            this.env = null;
            this.options = null;
            this.flushOptions = null;
            this.db = null;
        }
    }

    private static byte[] toBytes(long... args) {
        byte[] buf = new byte[args.length * Long.BYTES];
        ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(args);
        return buf;
    }

    private static long[] fromBytes(byte[] arg) {
        long[] arr = new long[arg.length / Long.BYTES];
        ByteBuffer.wrap(arg).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(arr);
        return arr;
    }

    @Test
    public void testSimple() throws RocksDBException {
        this.db.put("a".getBytes(), toBytes(5));
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 5 });
        this.db.flush(this.flushOptions);

        this.db.merge("a".getBytes(), toBytes(1, 0, 7));
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 5, 7 });

        this.db.merge("a".getBytes(), toBytes(0, 1, 5));
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 7 });

        this.db.compactRange();

        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 7 });
    }

    @Test
    public void testComplexPartialMerge() throws RocksDBException {
        this.db.put("a".getBytes(), toBytes(5, 6, 7));
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 5, 6, 7 });
        this.db.flush(this.flushOptions);

        this.db.merge("a".getBytes(), toBytes(3, 2, 1, 2, 3, 4, 5));
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 1, 2, 3, 6, 7 });

        this.db.merge("a".getBytes(), toBytes(2, 3, 2, 5, 1, 4, 7));
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 2, 3, 5, 6 });

        this.db.compactRange();
        assertArrayEquals(fromBytes(this.db.get("a".getBytes())), new long[]{ 2, 3, 5, 6 });
    }

    @Test
    public void testRandom() throws RocksDBException {
        String[] keys = { "a", "b", "c", "d" };
        Map<String, LongSortedSet> states = Stream.of(keys).collect(Collectors.toMap(Function.identity(), key -> new LongRBTreeSet()));

        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (int i = 0; i < 100000; i++) {
            String key = keys[r.nextInt(keys.length)];
            LongSortedSet state = states.get(key);

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

            this.db.merge(key.getBytes(), toBytes(buffer.array()));

            if (r.nextInt(100) == 0) {
                long[] expected = state.toLongArray();
                long[] found = fromBytes(PorkUtil.fallbackIfNull(this.db.get(key.getBytes()), new byte[0]));
                if (!Arrays.equals(expected, found)) {
                    throw new IllegalStateException(Arrays.toString(expected) + " != " + Arrays.toString(found));
                }
            }
        }

        for (String key : keys) {
            long[] expected = states.get(key).toLongArray();
            long[] found = fromBytes(PorkUtil.fallbackIfNull(this.db.get(key.getBytes()), new byte[0]));
            if (!Arrays.equals(expected, found)) {
                throw new IllegalStateException(Arrays.toString(expected) + " != " + Arrays.toString(found));
            }
        }

        this.db.compactRange();

        for (String key : keys) {
            long[] expected = states.get(key).toLongArray();
            long[] found = fromBytes(PorkUtil.fallbackIfNull(this.db.get(key.getBytes()), new byte[0]));
            if (!Arrays.equals(expected, found)) {
                throw new IllegalStateException(Arrays.toString(expected) + " != " + Arrays.toString(found));
            }
        }
    }
}
