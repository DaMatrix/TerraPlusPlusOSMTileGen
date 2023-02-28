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

package net.daporkchop.tpposmtilegen.util;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;

import java.util.ArrayList;
import java.util.List;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public final class TimedOperation implements AutoCloseable {
    private final Logger logger;
    private final long startTime = System.currentTimeMillis();

    private final List<TimedOperation> activeChildren = new ArrayList<>();
    private final TimedOperation parent;

    @Getter
    private volatile boolean done;

    public TimedOperation(@NonNull String name) {
        this(name, Logging.logger, null);
    }

    public TimedOperation(@NonNull String name, @NonNull Logger logger) {
        this(name, logger, null);
    }

    private TimedOperation(@NonNull String name, @NonNull Logger logger, TimedOperation parent) {
        if (parent != null) {
            checkState(!parent.done, "parent is already done!");
            parent.activeChildren.add(this);
        }

        this.logger = logger.channel(name);
        this.logger.info("Started...");
        this.parent = parent;
    }

    public TimedOperation pushChild(@NonNull String name) {
        checkState(!this.done, "already done?!?");
        return new TimedOperation(name, this.logger, this);
    }

    @Override
    public void close() {
        checkState(!this.done, "already done?!?");
        checkState(this.activeChildren.isEmpty() || this.activeChildren.stream().allMatch(TimedOperation::done), "%d children are not done!", this.activeChildren.size());
        this.done = true;

        if (this.parent != null) {
            checkState(this.parent.activeChildren.remove(this), "couldn't remove self from parent!");
        }

        long duration = System.currentTimeMillis() - this.startTime;
        this.logger.info(
                "Finished in %dh:%02dm:%02ds:%04dms",
                duration / (1000L * 60L * 60L),
                duration / (1000L * 60L) % 60L,
                duration / (1000L) % 60L,
                duration % 1000L
        );
    }
}
