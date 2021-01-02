package net.daporkchop.tpposmtilegen;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.mode.buildrefs.BuildRefs;
import net.daporkchop.tpposmtilegen.mode.digestpbf.DigestPBF;
import net.daporkchop.tpposmtilegen.mode.testindex.TestIndex;

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
        MODES.put("build_refs", BuildRefs::new);
        MODES.put("digest_pbf", DigestPBF::new);
        MODES.put("test_index", TestIndex::new);
    }

    public static void main(String... args) throws Exception {
        if (!PlatformInfo.IS_LITTLE_ENDIAN) {
            System.err.println("your processor must be little-endian!");
            return;
        }

        Supplier<IMode> modeFactory = MODES.get(args[0]);
        if (modeFactory == null) {
            System.err.printf("unknown mode: \"%s\"\n", args[0]);
            return;
        }

        FastThreadLocalThread thread = new FastThreadLocalThread((ERunnable) ()
                -> modeFactory.get().run(Arrays.copyOfRange(args, 1, args.length)), "main");
        thread.start();
    }
}
