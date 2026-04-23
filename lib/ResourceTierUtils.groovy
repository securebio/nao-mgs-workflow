// Helpers for processes whose peak memory scales strongly with input size.
// Files in lib/ are automatically loaded by Nextflow and callable from
// `memory` closures and `exec:` blocks.

import nextflow.util.MemoryUnit

class ResourceTierUtils {

    // Pick a memory tier for MASK_FASTQ_READS from the gzipped FASTQ size in bytes.
    // Empirical basis and threshold choice: see PR #737.
    static MemoryUnit maskFastqReadsMemory(long gzippedFastqBytes) {
        if (gzippedFastqBytes > 10L * 1024 * 1024 * 1024) return MemoryUnit.of('128 GB')
        if (gzippedFastqBytes >  2L * 1024 * 1024 * 1024) return MemoryUnit.of('64 GB')
        return MemoryUnit.of('32 GB')
    }
}
