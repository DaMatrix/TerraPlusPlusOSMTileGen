/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2021 DaPorkchop_
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

package net.daporkchop.tpposmtilegen.util;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class ProgressNotifier implements AutoCloseable {
    private static void append(@NonNull StringBuilder builder, long l) {
        if (l == 0L) {
            builder.append('0');
        } else {
            boolean firstSection = true;
            for (long div = 1_000_000_000L; div > 0L; div /= 1000L) {
                if (l / div > 0L) {
                    long section = (l / div) % 1000L;
                    if (firstSection) {
                        builder.append(section);
                    } else {
                        for (long j = 100L; j > 0L; j /= 10L) {
                            builder.append((section / j) % 10L);
                        }
                    }

                    if (div != 1L) {
                        builder.append('\'');
                    }

                    firstSection = false;
                }
            }
        }
    }

    protected final Slot[] slots;
    protected final Thread thread;

    protected final String prefix;
    protected final StringBuilder builder = new StringBuilder();

    protected final Logger logger;

    private ProgressNotifier(@NonNull Slot[] slots, @NonNull String prefix, long interval) {
        this.slots = slots;
        this.prefix = prefix;
        positive(interval, "interval");

        this.logger = Logging.logger.channel(prefix);

        this.thread = new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(interval);
                    this.print();
                }
            } catch (InterruptedException e) {
                //ignore
            }
        }, "progress notifier thread");
        this.thread.start();
    }

    public synchronized void print() {
        this.builder.setLength(0);

        for (Slot slot : this.slots) {
            this.builder.append(slot.name).append('=');

            append(this.builder, slot.sum());

            long total = slot.total.sum();
            if (total >= 0L) {
                this.builder.append('/');
                append(this.builder, total);
            }

            this.builder.append(',').append(' ');
        }
        this.builder.setLength(this.builder.length() - 2);

        this.logger.info(this.builder.toString());
    }

    public void step(int slot) {
        this.slots[slot].increment();
    }

    public ProgressNotifier setTotal(int slot, long total) {
        this.slots[slot].total.reset();
        this.slots[slot].total.add(total);
        return this;
    }

    public void incrementTotal(int slot) {
        this.slots[slot].total.increment();
    }

    @Override
    public void close() throws InterruptedException {
        this.thread.interrupt();
        this.thread.join();

        this.print();
        this.logger.success("Done.");
    }

    @AllArgsConstructor
    private static final class Slot extends LongAdder {
        @NonNull
        protected final String name;
        @NonNull
        protected final LongAdder total;
    }

    @Setter
    public static final class Builder {
        private final List<Slot> slots = new ArrayList<>();
        @NonNull
        private String prefix = "";
        private long interval = 5000L;

        public Builder slot(@NonNull String name) {
            return this.slot(name, -1L);
        }

        public Builder slot(@NonNull String name, long max) {
            LongAdder adder = new LongAdder();
            adder.add(max);
            this.slots.add(new Slot(name, adder));
            return this;
        }

        public ProgressNotifier build() {
            checkState(!this.slots.isEmpty(), "at least one slot must be defined!");
            return new ProgressNotifier(this.slots.toArray(new Slot[0]), this.prefix, this.interval);
        }
    }
}
