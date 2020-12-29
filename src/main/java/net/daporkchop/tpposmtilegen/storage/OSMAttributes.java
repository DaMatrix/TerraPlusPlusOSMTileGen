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

package net.daporkchop.tpposmtilegen.storage;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.MultiValueAttribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import lombok.experimental.UtilityClass;
import net.daporkchop.tpposmtilegen.util.LongArrayIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class OSMAttributes {
    public static final SimpleAttribute<Element, Long> ID = new SimpleAttribute<Element, Long>() {
        @Override
        public Long getValue(Element object, QueryOptions queryOptions) {
            return object.id;
        }
    };

    public static final SimpleAttribute<Element, Double> MIN_X = new SimpleAttribute<Element, Double>() {
        @Override
        public Double getValue(Element object, QueryOptions queryOptions) {
            return object.minX;
        }
    };

    public static final SimpleAttribute<Element, Double> MAX_X = new SimpleAttribute<Element, Double>() {
        @Override
        public Double getValue(Element object, QueryOptions queryOptions) {
            return object.maxX;
        }
    };

    public static final SimpleAttribute<Element, Double> MIN_Z = new SimpleAttribute<Element, Double>() {
        @Override
        public Double getValue(Element object, QueryOptions queryOptions) {
            return object.minZ;
        }
    };

    public static final SimpleAttribute<Element, Double> MAX_Z = new SimpleAttribute<Element, Double>() {
        @Override
        public Double getValue(Element object, QueryOptions queryOptions) {
            return object.maxZ;
        }
    };

    public static final Attribute<Element, Long> CHILDREN = new MultiValueAttribute<Element, Long>() {
        @Override
        public Iterable<Long> getValues(Element object, QueryOptions queryOptions) {
            if (object.children == null) {
                return Collections.emptyList();
            }

            return () -> new LongArrayIterator(object.children);
        }
    };

    public static final Attribute<Element, String> TAGS = new MultiValueAttribute<Element, String>() {
        @Override
        public Iterable<String> getValues(Element object, QueryOptions queryOptions) {
            if (object.tags == null) {
                return Collections.emptyList();
            }

            return () -> new Iterator<String>() {
                final Iterator<Map.Entry<String, String>> delegate = object.tags.entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return this.delegate.hasNext();
                }

                @Override
                public String next() {
                    Map.Entry<String, String> entry = this.delegate.next();
                    return entry.getKey() + '=' + entry.getValue();
                }
            };
        }
    };
}
