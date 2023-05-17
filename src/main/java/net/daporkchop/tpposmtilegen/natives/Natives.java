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

package net.daporkchop.tpposmtilegen.natives;

import lombok.experimental.UtilityClass;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.lib.natives.NativeFeature;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Natives {
    static {
        try {
            NativeFeature.loadNativeLibrary("", PolygonAssembler.class.getCanonicalName(), PolygonAssembler.class.getClassLoader());
            init(Logging.logger);

            //this isn't necessary, as librocksdbjni-linux64.so is implicitly loaded by our shared library (which depends on it)
            // RocksDB.loadLibrary();
        } catch (Throwable t) {
            final String[] EXTRA_LIBS = {
                    "librocksdbjni-linux64.so",
            };

            final Path[] CANDIDATE_NEW_DIRS = {
                    Paths.get("build/native-deps/shared").toAbsolutePath(),
            };

            String existingPath = System.getenv("LD_LIBRARY_PATH");
            List<Path> existingDirs = new ArrayList<>();
            if (existingPath != null) {
                for (String existingDir : existingPath.split(File.pathSeparator)) {
                    existingDirs.add(Paths.get(existingDir));
                }
            }
            existingDirs.add(Paths.get("/lib"));
            existingDirs.add(Paths.get("/usr/lib"));

            Set<String> requiredExtraDirs = new LinkedHashSet<>();
            OUTER:
            for (String extraLib : EXTRA_LIBS) {
                for (Path existingDir : existingDirs) {
                    if (Files.exists(existingDir.resolve(extraLib))) {
                        continue OUTER;
                    }
                }

                for (Path candidateNewDir : CANDIDATE_NEW_DIRS) {
                    if (Files.exists(candidateNewDir.resolve(extraLib))) {
                        requiredExtraDirs.add(candidateNewDir.toString());
                        continue OUTER;
                    }
                }

                t.addSuppressed(new IllegalStateException("couldn't find library '" + extraLib + "' on any of:\njava.library.path: " + existingDirs + "\ncandidate new directories: " + Arrays.toString(CANDIDATE_NEW_DIRS)));
            }

            throw new AssertionError("unable to load native libs!\nconsider adding '" + String.join(File.pathSeparator, requiredExtraDirs) + "' to the environment variable LD_LIBRARY_PATH", t);
        }
    }

    private static native void init(Logger logger);
}
