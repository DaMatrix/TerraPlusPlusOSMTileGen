package net.daporkchop.tpposmtilegen;

import net.daporkchop.tpposmtilegen.mode.countstrings.CountStringsMode;
import net.daporkchop.tpposmtilegen.pipeline.PipelineStep;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String... args) throws IOException {
        //new MemoryMappedSegmentedReader().read(new File(args[0]), new ToBytesParser(), System.out::write);
        //new StreamingSegmentedReader().read(new File(args[0]), new GeoJSONParser(), System.out::println);
        try (PipelineStep<File> step = new CountStringsMode().construct(Arrays.copyOfRange(args, 1, args.length))) {
            step.accept(new File(args[0]));
        }
    }
}
