// Helpers for processes whose peak memory scales strongly with input size.
// Files in lib/ are automatically loaded by Nextflow and callable from
// `memory` closures and `exec:` blocks.

import nextflow.util.MemoryUnit
import java.nio.file.Path

class ResourceTierUtils {

    // Pick a memory tier from process input(s) whose peak memory scales with total byte size.
    //   input:      a Path or List<Path> (a Nextflow `path(...)` process input). Byte sizes
    //               are summed across lists.
    //   thresholds: ascending byte boundaries as memory strings (e.g. ["2 GB", "10 GB"]).
    //   memories:   memory strings for each tier; size == thresholds.size() + 1.
    //               Returns memories[i] for the smallest i with bytes <= thresholds[i];
    //               falls back to memories.last() when bytes exceeds every threshold.
    static MemoryUnit pickMemoryTier(Object input, List<String> thresholds, List<String> memories) {
        if (memories.size() != thresholds.size() + 1) {
            throw new IllegalArgumentException(
                "pickMemoryTier: memories.size() (${memories.size()}) must equal " +
                "thresholds.size() + 1 (${thresholds.size() + 1})")
        }
        long bytes = (input instanceof List)
            ? ((List) input).sum { ((Path) it).size() }
            : ((Path) input).size()
        for (int i = 0; i < thresholds.size(); i++) {
            if (bytes <= MemoryUnit.of(thresholds[i]).toBytes()) return MemoryUnit.of(memories[i])
        }
        return MemoryUnit.of(memories.last())
    }
}
