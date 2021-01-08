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

package net.daporkchop.tpposmtilegen.osm.changeset;

import io.netty.buffer.ByteBuf;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
@ToString
@EqualsAndHashCode
public class ChangesetState {
    protected final String timestamp;
    protected final int sequenceNumber;

    public ChangesetState(@NonNull ByteBuf data) {
        String timestamp = null;
        int sequenceNumber = -1;

        Matcher matcher = Pattern.compile("^(.*?)=(.+)$", Pattern.MULTILINE)
                .matcher(data.readCharSequence(data.readableBytes(), StandardCharsets.US_ASCII));
        while (matcher.find()) {
            switch (matcher.group(1)) {
                case "timestamp":
                    timestamp = matcher.group(2).replace("\\:", ":");
                    break;
                case "sequenceNumber":
                    sequenceNumber = Integer.parseInt(matcher.group(2));
                    break;
                default:
                    System.err.println("Skipping unknown entry: " + matcher.group());
            }
        }

        checkState(timestamp != null, "timestamp not set!");
        checkState(sequenceNumber >= 0, "sequenceNumber not set!");

        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }
}
