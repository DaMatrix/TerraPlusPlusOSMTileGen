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

package net.daporkchop.tpposmtilegen.mode;

import io.netty.util.AsciiString;
import lombok.NonNull;
import net.daporkchop.lib.common.function.throwing.EBiConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.ref.Ref;
import net.daporkchop.lib.common.ref.ThreadRef;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.tpposmtilegen.geometry.Geometry;
import net.daporkchop.tpposmtilegen.storage.Storage;
import net.daporkchop.tpposmtilegen.util.ProgressNotifier;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class Test implements IMode {
    @Override
    public String name() {
        return "test";
    }

    @Override
    public String synopsis() {
        return "<index_dir>";
    }

    @Override
    public String help() {
        return "just debug code";
    }

    @Override
    public void run(@NonNull String... args) throws Exception {
        checkArg(args.length == 1, "Usage: test <index_dir>");
        File src = PFiles.assertDirectoryExists(new File(args[0]));

        //stupidly inefficient code to fix broken file names
        try (Storage storage = new Storage(src.toPath())) {
            Ref<Matcher> regexCache = ThreadRef.regex(Pattern.compile("(0/(?:coastline|(?:relation|way)/\\d{3})/\\d{3}/)(\\d{1,2})\\.json"));
            Ref<StringBuffer> bufferCache = ThreadRef.late(StringBuffer::new);
            try (ProgressNotifier notifier = new ProgressNotifier.Builder().prefix("Fix external file names").slot("skipped").slot("renamed").slot("modified").slot("modified and renamed").build()) {
                storage.files().forEachParallel(storage.db().read(), (EBiConsumer<String, ByteBuffer>) (path, buffer) -> {
                    try {
                        Matcher matcher = regexCache.get();

                        boolean content = false;

                        String replacedContent = this.replace(bufferCache, matcher.reset(new AsciiString(buffer.array(), false)));
                        if (replacedContent != null) {
                            buffer = Geometry.toBytes(replacedContent);
                            content = true;
                        }

                        String replacedName = this.replace(bufferCache, matcher.reset(path));
                        boolean name = replacedName != null;

                        if (content | name) {
                            if (buffer.hasArray()) {
                                buffer = ByteBuffer.allocateDirect(buffer.array().length).put(buffer.array());
                                buffer.flip();
                            }
                            storage.files().put(storage.db().batch(), name ? replacedName : path, buffer);
                            if (name) {
                                storage.files().delete(storage.db().batch(), path);
                            }
                        }

                        if (content && name) {
                            notifier.step(3);
                        } else if (content) {
                            notifier.step(2);
                        } else if (name) {
                            notifier.step(1);
                        } else {
                            notifier.step(0);
                        }
                    } finally {
                        if (!buffer.hasArray()) {
                            PUnsafe.pork_releaseBuffer(buffer);
                        }
                    }
                });
            }
        }
    }

    private String replace(@NonNull Ref<StringBuffer> bufCache, @NonNull Matcher matcher) {
        if (matcher.find()) {
            StringBuffer buf = bufCache.get();
            buf.setLength(0);
            do {
                matcher.appendReplacement(buf, matcher.group(1) + String.format("%03d", Integer.parseUnsignedInt(matcher.group(2))) + ".json");
            } while (matcher.find());
            matcher.appendTail(buf);
            return buf.toString();
        }
        return null;
    }
}
