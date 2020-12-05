package net.daporkchop.tpposmtilegen;

import net.daporkchop.tpposmtilegen.input.parse.ToBytesParser;
import net.daporkchop.tpposmtilegen.input.read.SegmentedReader;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException {
        new SegmentedReader().read(new File(args[0]), new ToBytesParser(), System.out::write);
    }
}
