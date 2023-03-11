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

package net.daporkchop.tpposmtilegen.storage.special;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.storage.rocksdb.Database;
import net.daporkchop.tpposmtilegen.storage.rocksdb.WrappedRocksDB;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBReadAccess;
import net.daporkchop.tpposmtilegen.storage.rocksdb.access.DBWriteAccess;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalLong;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class DBProperties extends WrappedRocksDB {
    public DBProperties(@NonNull Database database, @NonNull ColumnFamilyHandle column, @NonNull ColumnFamilyDescriptor desc) {
        super(database, column, desc);
    }

    public StringProperty getStringProperty(@NonNull String name) throws Exception {
        return new StringProperty(name);
    }

    public LongProperty getLongProperty(@NonNull String name) throws Exception {
        return new LongProperty(name);
    }

    /**
     * @author DaPorkchop_
     */
    public interface Property<T> {
        String name();

        boolean isPresent(@NonNull DBReadAccess access) throws Exception;

        void set(@NonNull DBWriteAccess access, @NonNull T value) throws Exception;

        Optional<T> get(@NonNull DBReadAccess access) throws Exception;

        void remove(@NonNull DBWriteAccess access) throws Exception;
    }

    /**
     * @author DaPorkchop_
     */
    @Getter
    protected abstract class AbstractProperty<T> implements Property<T> {
        protected final String name;
        protected final byte[] key;

        public AbstractProperty(@NonNull String name) {
            this.name = name;
            this.key = name.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public boolean isPresent(@NonNull DBReadAccess access) throws Exception {
            return access.get(DBProperties.this.column, this.key) != null;
        }

        @Override
        public void remove(@NonNull DBWriteAccess access) throws Exception {
            access.delete(DBProperties.this.column, this.key);
        }
    }

    /**
     * @author DaPorkchop_
     */
    public final class StringProperty extends AbstractProperty<String> {
        public StringProperty(@NonNull String name) {
            super(name);
        }

        @Override
        public void set(@NonNull DBWriteAccess access, @NonNull String value) throws Exception {
            access.put(DBProperties.this.column, this.key, value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Optional<String> get(@NonNull DBReadAccess access) throws Exception {
            return Optional.ofNullable(access.get(DBProperties.this.column, this.key))
                    .map(valueArray -> new String(valueArray, StandardCharsets.UTF_8));
        }
    }

    /**
     * @author DaPorkchop_
     */
    public final class LongProperty extends AbstractProperty<Long> {
        public LongProperty(@NonNull String name) {
            super(name);
        }

        @Override
        @Deprecated
        public void set(@NonNull DBWriteAccess access, @NonNull Long value) throws Exception {
            this.set(access, value.longValue());
        }

        public void set(@NonNull DBWriteAccess access, long value) throws Exception {
            ByteArrayRecycler valueArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
            byte[] valueArray = valueArrayRecycler.get();
            try {
                PUnsafe.putUnalignedLongLE(valueArray, PUnsafe.arrayByteElementOffset(0), value);
                access.put(DBProperties.this.column, this.key, valueArray);
            } finally {
                valueArrayRecycler.release(valueArray);
            }
        }

        @Override
        @Deprecated
        public Optional<Long> get(@NonNull DBReadAccess access) throws Exception {
            OptionalLong value = this.getLong(access);
            return value.isPresent() ? Optional.of(value.getAsLong()) : Optional.empty();
        }

        public OptionalLong getLong(@NonNull DBReadAccess access) throws Exception {
            byte[] valueArray = access.get(DBProperties.this.column, this.key);
            if (valueArray != null) {
                checkState(valueArray.length == Long.BYTES);
                return OptionalLong.of(PUnsafe.getUnalignedLongLE(valueArray, PUnsafe.arrayByteElementOffset(0)));
            } else {
                return OptionalLong.empty();
            }
        }

        public void increment(@NonNull DBWriteAccess access) throws Exception {
            this.add(access, 1L);
        }

        public void decrement(@NonNull DBWriteAccess access) throws Exception {
            this.add(access, -1L);
        }

        public void add(@NonNull DBWriteAccess access, long value) throws Exception {
            if (value == 0L) {
                return; //nothing to be done
            }

            ByteArrayRecycler valueArrayRecycler = BYTE_ARRAY_RECYCLER_8.get();
            byte[] valueArray = valueArrayRecycler.get();
            try {
                PUnsafe.putUnalignedLongLE(valueArray, PUnsafe.arrayByteElementOffset(0), value);
                access.merge(DBProperties.this.column, this.key, valueArray);
            } finally {
                valueArrayRecycler.release(valueArray);
            }
        }
    }
}
