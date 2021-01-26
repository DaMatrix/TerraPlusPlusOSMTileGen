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

package net.daporkchop.tpposmtilegen;

import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.PFunctions;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.lib.logging.LogAmount;
import net.daporkchop.lib.logging.format.FormatParser;
import net.daporkchop.tpposmtilegen.mode.Assemble;
import net.daporkchop.tpposmtilegen.mode.Compact;
import net.daporkchop.tpposmtilegen.mode.DigestCoastlines;
import net.daporkchop.tpposmtilegen.mode.DigestPBF;
import net.daporkchop.tpposmtilegen.mode.Export;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.mode.Purge;
import net.daporkchop.tpposmtilegen.mode.RecomputeReferences;
import net.daporkchop.tpposmtilegen.mode.Test;
import net.daporkchop.tpposmtilegen.mode.Update;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
public final class Main implements Runnable {
    protected static final Map<String, IMode> MODES = Stream.of(
            new Assemble(),
            new Compact(),
            new DigestCoastlines(),
            new DigestPBF(),
            new Export(),
            new Purge(),
            new RecomputeReferences(),
            new Test(),
            new Update()
    ).collect(Collectors.toMap(IMode::name, PFunctions.identity()));

    public static void main(String... args) {
        Thread.setDefaultUncaughtExceptionHandler((thread, t) -> {
            logger.alert("Uncaught exception in thread %s!", t, thread);
            System.exit(1);
        });

        logger.redirectStdOut().enableANSI()
                .addFile(new File("logs/" + Instant.now() + ".log"), LogAmount.DEBUG)
                .setLogAmount(LogAmount.DEBUG);

        logger.info(""
                    + "terra++ OpenStreetMap tile generator\n"
                    + "Copyright (c) 2020-2021 DaPorkchop_\n"
                    + "  https://daporkchop.net\n");

        //relaunch main thread as a FastThreadLocalThread
        new FastThreadLocalThread(new Main(args), "main").start();
    }

    @NonNull
    private final String[] args;

    @Override
    public void run() {
        if (!PlatformInfo.IS_LITTLE_ENDIAN) {
            logger.alert("Your processor must be little-endian!");
            System.exit(1);
        }

        IMode mode = MODES.get(this.args[0]);
        if (mode == null) {
            logger.error("Unknown mode: %s", this.args[0]);
            this.printHelp();
            System.exit(1);
        }

        String[] modeArgs = Arrays.copyOfRange(this.args, 1, this.args.length);
        try {
            mode.run(modeArgs);
        } catch (IllegalArgumentException e) {
            logger.alert("Invalid arguments!\n%s", e, String.join("\n", modeArgs));
            this.printUsage(mode);
            System.exit(1);
        } catch (Exception e) {
            logger.alert("Exception while running %s with arguments:\n%s", e, this.args[0], String.join("\n", modeArgs));
            System.exit(1);
        }
    }

    private void printHelp() {
        logger.info(""
                    + "Usage:\n"
                    + "  ./T++OSMTileGen/bin/T++OSMTileGen <mode_name> [args...]\n"
                    + '\n'
                    + "Modes:");

        FormatParser format = this.prefix("  ");

        MODES.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .forEach(this::printUsage);

        logger.setFormatParser(format); //restore format
    }

    private void printUsage(@NonNull IMode mode) {
        logger.info("%s:", mode.name());

        FormatParser format = this.prefix("  ");

        logger.info(""
                    + "Usage:\n"
                    + "  %s %s\n"
                    + '\n'
                    + "%s",
                mode.name(), mode.synopsis(), mode.help());

        logger.setFormatParser(format); //restore format
    }

    private FormatParser prefix(@NonNull String prefix) {
        FormatParser format = logger.getFormatParser();
        logger.setFormatParser(s -> format.parse(prefix + s));
        return format;
    }
}
