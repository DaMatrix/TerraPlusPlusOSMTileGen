package net.daporkchop.tpposmtilegen;

import net.daporkchop.tpposmtilegen.pipeline.Parallelizer;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;
import net.daporkchop.tpposmtilegen.pipeline.parse.GeoJSONParser;
import net.daporkchop.tpposmtilegen.pipeline.read.StreamingSegmentedReader;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException {
        //new MemoryMappedSegmentedReader().read(new File(args[0]), new ToBytesParser(), System.out::write);
        //new StreamingSegmentedReader().read(new File(args[0]), new GeoJSONParser(), System.out::println);
        try (PipelineStep<File> step = new StreamingSegmentedReader(new Parallelizer<>(new GeoJSONParser(o -> {})))) {
            step.accept(new File(args[0]));
        }
    }
}
