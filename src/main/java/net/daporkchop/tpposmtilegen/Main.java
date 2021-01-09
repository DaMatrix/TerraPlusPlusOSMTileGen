package net.daporkchop.tpposmtilegen;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.daporkchop.lib.common.system.PlatformInfo;
import net.daporkchop.tpposmtilegen.mode.DigestCoastlines;
import net.daporkchop.tpposmtilegen.mode.DigestPBF;
import net.daporkchop.tpposmtilegen.mode.IMode;
import net.daporkchop.tpposmtilegen.mode.RebuildPlanet;
import net.daporkchop.tpposmtilegen.mode.Compact;
import net.daporkchop.tpposmtilegen.mode.Update;

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
        MODES.put("compact", Compact::new);
        MODES.put("digest_coastlines", DigestCoastlines::new);
        MODES.put("digest_pbf", DigestPBF::new);
        MODES.put("rebuild_planet", RebuildPlanet::new);
        MODES.put("update", Update::new);
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

        FastThreadLocalThread thread = new FastThreadLocalThread(() -> {
            try {
                modeFactory.get().run(Arrays.copyOfRange(args, 1, args.length));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }, "main");
        thread.start();
    }
}
