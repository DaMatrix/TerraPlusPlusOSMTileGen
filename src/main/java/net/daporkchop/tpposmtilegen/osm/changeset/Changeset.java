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

package net.daporkchop.tpposmtilegen.osm.changeset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@Getter
@ToString
public final class Changeset {
    private static final XmlMapper XML_MAPPER = new XmlMapper();

    public static Changeset parse(@NonNull byte[] data) throws IOException {
        return parse(Unpooled.wrappedBuffer(data));
    }

    public static Changeset parse(@NonNull ByteBuf buf) throws IOException {
        try (InputStream in = new BufferedInputStream(new GZIPInputStream(new ByteBufInputStream(buf)))) {
            return XML_MAPPER.readValue(in, Changeset.class);
        }
    }

    @JsonProperty
    protected String version;

    @JsonProperty
    protected String generator;

    protected final List<Entry> entries = new ArrayList<>();

    @JsonSetter("create")
    private void create(@NonNull Entry entry) {
        this.entries.add(entry.op(Operation.CREATE));
    }

    @JsonSetter("modify")
    private void modify(@NonNull Entry entry) {
        this.entries.add(entry.op(Operation.MODIFY));
    }

    @JsonSetter("delete")
    private void delete(@NonNull Entry entry) {
        this.entries.add(entry.op(Operation.DELETE));
    }

    @Getter
    @ToString
    public static final class Entry {
        @Setter(AccessLevel.PRIVATE)
        protected Operation op;

        protected final List<Element> elements = new ArrayList<>();

        @JsonSetter("node")
        private void node(Node node) {
            checkArg(node.version() >= 0, "node doesn't have a version! %s", node);
            this.elements.add(node);
        }

        @JsonSetter("way")
        private void way(Way way) {
            checkArg(way.version() >= 0, "way doesn't have a version! %s", way);
            this.elements.add(way);
        }

        @JsonSetter("relation")
        private void relation(Relation relation) {
            checkArg(relation.version() >= 0, "relation doesn't have a version! %s", relation);
            this.elements.add(relation);
        }
    }

    @Getter
    @ToString
    @JsonIgnoreProperties({"changeset", "uid", "user"})
    public static abstract class Element {
        @JsonProperty
        protected long id;

        @JsonProperty
        protected int version = -1;

        @JsonProperty
        protected int changeset = -1;

        protected Instant timestamp;

        protected final Map<String, String> tags = new HashMap<>();

        public abstract int type();

        @JsonSetter("timestamp")
        private void timestamp(String timestamp) {
            this.timestamp = Instant.parse(timestamp);
        }

        @JsonSetter("tag")
        private void tag(Tag tag) {
            this.tags.put(tag.k, tag.v);
        }

        private static final class Tag {
            private final String k;
            private final String v;

            @JsonCreator
            public Tag(@JsonProperty(value = "k", required = true) String k,
                       @JsonProperty(value = "v", required = true) String v) {
                this.k = k;
                this.v = v;
            }
        }
    }

    @Getter
    @ToString(callSuper = true)
    public static final class Node extends Element {
        @JsonProperty
        protected String lon;
        @JsonProperty
        protected String lat;

        @Override
        public int type() {
            return net.daporkchop.tpposmtilegen.osm.Node.TYPE;
        }
    }

    @Getter
    @ToString(callSuper = true)
    public static final class Way extends Element {
        protected final LongList refs = new LongArrayList();

        @Override
        public int type() {
            return net.daporkchop.tpposmtilegen.osm.Way.TYPE;
        }

        @JsonSetter("nd")
        private void nd(Nd nd) {
            this.refs.add(nd.ref);
        }

        private static final class Nd {
            private final long ref;

            @JsonCreator
            public Nd(@JsonProperty(value = "ref", required = true) long ref) {
                this.ref = ref;
            }
        }
    }

    @Getter
    @ToString(callSuper = true)
    public static final class Relation extends Element {
        protected final List<Member> members = new ArrayList<>();

        @Override
        public int type() {
            return net.daporkchop.tpposmtilegen.osm.Relation.TYPE;
        }

        @JsonSetter("member")
        private void member(Member member) {
            this.members.add(member);
        }

        @Getter
        @ToString
        public static final class Member {
            private final String type;
            private final long ref;
            private final String role;

            @JsonCreator
            public Member(@JsonProperty(value = "type", required = true) String type,
                          @JsonProperty(value = "ref", required = true) long ref,
                          @JsonProperty(value = "role", required = true) String role) {
                this.type = type;
                this.ref = ref;
                this.role = role;
            }
        }
    }
}
