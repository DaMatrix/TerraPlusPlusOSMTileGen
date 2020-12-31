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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * See <a href="https://github.com/osmlab/id-area-keys">osmlab/id-area-keys</a>.
 *
 * @author DaPorkchop_
 */
@UtilityClass
public class AreaKeys {
    protected final Map<String, Set<String>> AREA_KEYS;
    protected final Predicate<Map.Entry<String, String>> FILTER;

    static {
        try (InputStream in = AreaKeys.class.getResourceAsStream("/areakeys.json")) {
            ImmutableMap.Builder<String, Set<String>> mapBuilder = ImmutableMap.builder();
            new JsonMapper().readTree(in).fields().forEachRemaining(entry0 -> {
                ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();
                entry0.getValue().fields().forEachRemaining(entry1 -> setBuilder.add(entry1.getKey().intern()));
                mapBuilder.put(entry0.getKey().intern(), setBuilder.build());
            });
            AREA_KEYS = mapBuilder.build();
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        FILTER = entry -> {
            Set<String> set = AREA_KEYS.get(entry.getKey());
            return set != null && !set.contains(entry.getValue());
        };
    }

    public boolean isArea(@NonNull Map<String, String> tags) {
        String area = tags.get("area");
        if (area != null) {
            if ("yes".equals(area)) {
                return true;
            } else if ("no".equals(area)) {
                return false;
            }
        }

        return tags.entrySet().stream().anyMatch(FILTER);
    }
}
