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

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.binary.stream.DataIn;
import net.daporkchop.lib.compression.context.PInflater;
import net.daporkchop.lib.compression.zstd.Zstd;
import net.daporkchop.lib.encoding.Hexadecimal;

import java.io.File;
import java.io.IOException;

/**
 * Global utility methods.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class Util {
    public static final JsonMapper JSON_MAPPER = new JsonMapper();

    public static DataIn readerFor(@NonNull File file) throws IOException {
        DataIn in = DataIn.wrapNonBuffered(file);
        if (file.getName().endsWith(".zst")) {
            PInflater inflater = Zstd.PROVIDER.inflater();
            in = inflater.decompressionStream(in);
            inflater.release(); //release now so that the only remaining reference comes from the DataIn instance
        }
        return in;
    }

    public static int readJsonStringToEnd(int i, @NonNull StringBuilder dst, @NonNull ByteBuf src) {
        while (true) {
            int b = src.getUnsignedByte(i++);
            if ((b & 0x80) == 0) {
                if (b == '"') { //end of string
                    break;
                } else if (b == '\\') { //escape sequence
                    switch (b = src.getUnsignedByte(i++)) {
                        case '"':
                        case '\\':
                        case '/':
                            dst.append((char) b);
                            break;
                        case 'n':
                            dst.append('\n');
                            break;
                        case 't':
                            dst.append('\t');
                            break;
                        case 'r':
                            dst.append('\r');
                            break;
                        case 'f':
                            dst.append('\f');
                            break;
                        case 'b':
                            dst.append('\b');
                            break;
                        case 'u':
                            char c = (char) ((Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)) << 8)
                                             | Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)));
                            if (!Character.isHighSurrogate(c)) {
                                dst.append(c);
                            } else {
                                i += 2;
                                char c2 = (char) ((Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)) << 8)
                                                  | Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)));
                                dst.appendCodePoint(Character.toCodePoint(c, c2));
                            }
                            break;
                    }
                } else { //normal ASCII character
                    dst.append((char) b);
                }
            } else if ((b & 0xE0) == 0xC0) { //UTF-8, 2 bytes
                dst.append((char) (((b & 0x1F) << 6)
                                   | (src.getUnsignedByte(i++) & 0x3F)));
            } else if ((b & 0xF0) == 0xE0) { //UTF-8, 3 bytes
                dst.append((char) (((b & 0xF) << 12)
                                   | ((src.getUnsignedByte(i++) & 0x3F) << 6)
                                   | (src.getUnsignedByte(i++) & 0x3F)));
            } else if ((b & 0xF0) == 0xF0) { //UTF-8, 4 bytes
                dst.appendCodePoint(((b & 0xF) << 18)
                                    | ((src.getUnsignedByte(i++) & 0x3F) << 12)
                                    | ((src.getUnsignedByte(i++) & 0x3F) << 6)
                                    | (src.getUnsignedByte(i++) & 0x3F));
            }
        }
        return i;
    }

    public static int readJsonStringToEnd(int i, @NonNull ByteBuf dst, @NonNull ByteBuf src) {
        while (true) {
            int b = src.getUnsignedByte(i++);
            if (b == '"') { //end of string
                break;
            } else if (b != '\\') { //not an escape sequence
                dst.writeByte(b);
                if ((b & 0x80) == 0) {
                } else if ((b & 0xE0) == 0xC0) { //UTF-8, 2 bytes
                    dst.writeByte(src.getUnsignedByte(i++));
                } else if ((b & 0xF0) == 0xE0) { //UTF-8, 3 bytes
                    dst.writeByte(src.getUnsignedByte(i++))
                            .writeByte(src.getUnsignedByte(i++));
                } else if ((b & 0xF0) == 0xF0) { //UTF-8, 4 bytes
                    dst.writeByte(src.getUnsignedByte(i++))
                            .writeByte(src.getUnsignedByte(i++))
                            .writeByte(src.getUnsignedByte(i++));
                }
            } else { //escape sequence
                switch (b = src.getUnsignedByte(i++)) {
                    case '"':
                    case '\\':
                    case '/':
                        dst.writeByte(b);
                        break;
                    case 'n':
                        dst.writeByte('\n');
                        break;
                    case 't':
                        dst.writeByte('\t');
                        break;
                    case 'r':
                        dst.writeByte('\r');
                        break;
                    case 'f':
                        dst.writeByte('\f');
                        break;
                    case 'b':
                        dst.writeByte('\b');
                        break;
                    case 'u':
                        char c = (char) ((Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)) << 8)
                                         | Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)));
                        if (c < 0x80) {
                            dst.writeByte(c);
                        } else if (c < 0x800) {
                            dst.writeByte(0xC0 | (c >> 6))
                                    .writeByte(0x80 | (c & 0x3F));
                        } else if (Character.isHighSurrogate(c)) {
                            i += 2;
                            char c2 = (char) ((Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)) << 8)
                                              | Hexadecimal.decodeUnsigned((char) src.getUnsignedByte(i++), (char) src.getUnsignedByte(i++)));
                            int codePoint = Character.toCodePoint(c, c2);
                            dst.writeByte(0xF0 | (codePoint >> 18))
                                    .writeByte(0x80 | ((codePoint >> 12) & 0x3F))
                                    .writeByte(0x80 | ((codePoint >> 6) & 0x3F))
                                    .writeByte(0x80 | (codePoint & 0x3F));
                        } else {
                            dst.writeByte(0xE0 | (c >> 12))
                                    .writeByte(0x80 | ((c >> 6) & 0x3F))
                                    .writeByte(0x80 | (c & 0x3F));
                        }
                        break;
                }
            }
        }
        dst.writeByte(0); //NUL terminator
        return i;
    }
}
