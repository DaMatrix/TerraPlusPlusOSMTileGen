package net.daporkchop.tpposmtilegen;

import net.daporkchop.tpposmtilegen.input.parse.GeoJSONParser;
import net.daporkchop.tpposmtilegen.input.read.MemoryMappedSegmentedReader;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException {
        //new MemoryMappedSegmentedReader().read(new File(args[0]), new ToBytesParser(), System.out::write);
        //new MemoryMappedSegmentedReader().read(new File(args[0]), new GeoJSONParser(), System.out::println);
        new MemoryMappedSegmentedReader().read(new File(args[0]), new GeoJSONParser(), obj -> {});
    }
}
