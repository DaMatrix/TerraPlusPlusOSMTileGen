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

package net.daporkchop.tpposmtilegen.util;

import lombok.NonNull;
import net.daporkchop.lib.common.pool.handle.Handle;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class ProgressNotifier implements AutoCloseable {
    protected final LongAdder[] counters;
    protected final String[] names;
    protected final Thread thread;

    protected final String prefix;
    protected final StringBuilder builder = new StringBuilder();

    public ProgressNotifier(@NonNull String prefix, long interval, @NonNull String... names) {
        this.prefix = prefix;
        positive(interval, "interval");
        positive(names.length, "names.length");
        this.names = names.clone();
        this.counters = Arrays.stream(names).peek(Objects::requireNonNull).map(s -> new LongAdder()).toArray(LongAdder[]::new);

        this.thread = new Thread(() -> {
            try {
                while (true) {
                    this.print();
                    Thread.sleep(interval);
                }
            } catch (InterruptedException e) {
                //ignore
            } finally {
                System.out.println("Done!");
            }
        }, "progress notifier thread");
        this.thread.start();
    }

    public synchronized void print() {
        this.builder.setLength(0);
        this.builder.append(this.prefix);

        for (int i = 0; i < this.names.length; i++) {
            this.builder.append(this.names[i]).append('=');

            long l = this.counters[i].sum();
            if (l == 0L) {
                this.builder.append('0');
            } else {
                boolean firstSection = true;
                for (long div = 1_000_000L; div > 0L; div /= 1000L) {
                    if (l / div > 0L) {
                        long section = (l / div) % 1000L;
                        if (firstSection) {
                            this.builder.append(section);
                        } else {
                            for (long j = 100L; j > 0L; j /= 10L) {
                                this.builder.append((section / j) % 10L);
                            }
                        }

                        if (div != 1L) {
                            this.builder.append('\'');
                        }

                        firstSection = false;
                    }
                }
            }

            if (i + 1 != this.names.length) {
                this.builder.append(", ");
            }
        }
        System.out.println(this.builder);
    }

    public void step(int slot) {
        this.counters[slot].increment();
    }

    @Override
    public void close() {
        this.thread.interrupt();
    }
}
