package net.daporkchop.tpposmtilegen;

import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.mode.assemblebvh.AssembleBVH;
import net.daporkchop.tpposmtilegen.mode.countstrings.CountStrings;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author DaPorkchop_
 */
public class Main {
    protected static final Map<String, Supplier<IMode>> MODES = new HashMap<>();

    static {
        MODES.put("assemble_bvh", AssembleBVH::new);
        MODES.put("count_strings", CountStrings::new);
    }

    public static void main(String... args) throws IOException {
        Supplier<IMode> modeFactory = MODES.get(args[0]);
        if (modeFactory == null) {
            System.err.printf("unknown mode: \"%s\"\n", args[0]);
            return;
        }

        modeFactory.get().run(Arrays.copyOfRange(args, 1, args.length));
    }
}
