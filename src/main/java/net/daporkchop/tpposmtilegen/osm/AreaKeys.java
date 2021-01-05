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

package net.daporkchop.tpposmtilegen.osm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static net.daporkchop.lib.common.util.PorkUtil.*;

/**
 * @author DaPorkchop_
 */
//TODO: use filters from https://github.com/drolbr/Overpass-API/blob/dc125141a0b9435da6e273bfbe09af7fa19b283c/src/rules/areas.osm3s
@UtilityClass
public class AreaKeys {
    protected final Predicate<Map<String, String>> RELATION_FILTER = parseFilter("relations");
    protected final Predicate<Map<String, String>> WAY_FILTER = parseFilter("ways");

    public static boolean isRelationArea(@NonNull Map<String, String> tags) {
        return !tags.isEmpty() && RELATION_FILTER.test(tags);
    }

    public static boolean isWayArea(@NonNull Map<String, String> tags) {
        if (tags.isEmpty()) {
            return false;
        }

        String area = tags.get("area");
        if (area != null) {
            if ("yes".equals(area)) {
                return true;
            } else if ("no".equals(area)) {
                return false;
            }
        }

        return WAY_FILTER.test(tags);
    }

    private Predicate<Map<String, String>> parseFilter(@NonNull String name) {
        try (InputStream in = AreaKeys.class.getResourceAsStream(name + ".json")) {
            return parseFilter(new JsonMapper().readTree(in));
        } catch (IOException e) {
            throw new RuntimeException(name, e);
        }
    }

    private Predicate<Map<String, String>> parseFilter(@NonNull JsonNode root) {
        if (root.isArray()) {
            Predicate<Map<String, String>>[] filters = uncheckedCast(StreamSupport.stream(root.spliterator(), false)
                    .map(AreaKeys::parseFilter)
                    .toArray(Predicate[]::new));

            return tags -> {
                for (Predicate<Map<String, String>> filter : filters) {
                    if (filter.test(tags)) {
                        return true;
                    }
                }
                return false;
            };
        } else {
            Predicate<Map<String, String>>[] filters = uncheckedCast(StreamSupport.stream(Spliterators.spliteratorUnknownSize(root.fields(), 0), false)
                    .map((Function<Map.Entry<String, JsonNode>, Predicate<Map<String, String>>>) entry -> {
                        String key = entry.getKey().intern();
                        if (entry.getValue().isArray()) {
                            Set<String> values = ImmutableSet.copyOf(StreamSupport.stream(entry.getValue().spliterator(), false)
                                    .map(JsonNode::asText).map(String::intern).collect(Collectors.toList()));
                            return tags -> {
                                String v = tags.get(key);
                                return v != null && values.contains(v);
                            };
                        } else if (entry.getValue().isTextual()) {
                            String value = entry.getValue().asText().intern();
                            return tags -> {
                                String v = tags.get(key);
                                return value.equals(v);
                            };
                        } else if (entry.getValue().isNull()) {
                            return tags -> tags.get(key) != null;
                        } else {
                            throw new IllegalArgumentException(entry.toString());
                        }
                    })
                    .toArray(Predicate[]::new));

            return tags -> {
                for (Predicate<Map<String, String>> filter : filters) {
                    if (!filter.test(tags)) {
                        return false;
                    }
                }
                return true;
            };
        }
    }
}
