package net.daporkchop.tpposmtilegen;

import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.mode.countstrings.CountStringsMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class Main {
    protected static final Map<String, Supplier<IMode>> MODES = new HashMap<>();

    static {
        MODES.put("count_strings", CountStringsMode::new);
    }

    public static void main(String... args) throws IOException {
        //new MemoryMappedSegmentedReader().read(new File(args[0]), new ToBytesParser(), System.out::write);
        //new StreamingSegmentedReader().read(new File(args[0]), new GeoJSONParser(), System.out::println);

        Supplier<IMode> modeFactory = MODES.get(args[0]);
        if (modeFactory == null) {
            System.err.printf("unknown mode: \"%s\"\n", args[0]);
            return;
        }

        modeFactory.get().run(Arrays.copyOfRange(args, 1, args.length));
    }
}
