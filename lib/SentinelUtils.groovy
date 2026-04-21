// Shared helpers for the WRITE_SENTINEL and WRITE_SENTINEL_DOWNSTREAM modules.
// Files in lib/ are automatically loaded by Nextflow and callable from exec: blocks.

class SentinelUtils {

    // Parse expected-outputs-* lists from pyproject.toml text and expand a wildcard.
    //   pyprojectText : full contents of pyproject.toml
    //   keys          : [tool.mgs-workflow] suffixes to concatenate (e.g. ["run", "run-shortread-extra"])
    //   wildcard      : placeholder to expand (e.g. "SAMPLE" or "GROUP")
    //   names         : values to substitute for each wildcard occurrence
    // Assumes array values in pyproject.toml do not contain literal ] characters.
    static List<String> getExpectedOutputs(String pyprojectText, List<String> keys,
                                            String wildcard, List<String> names) {
        def expected = []
        def placeholder = "{${wildcard}}"
        for (k in keys) {
            def fullKey = "expected-outputs-${k}"
            def sectionMatch = (pyprojectText =~ /(?s)${fullKey} = \[(.*?)\]/)
            if (sectionMatch) {
                def patterns = (sectionMatch[0][1] =~ /"([^"]+)"/).collect { it[1] }
                for (pattern in patterns) {
                    if (pattern.contains(placeholder)) {
                        for (name in names) {
                            expected.add(pattern.replace(placeholder, name))
                        }
                    } else {
                        expected.add(pattern)
                    }
                }
            }
        }
        return expected.sort().unique()
    }

    // Poll outputDir for each expected file with exponential backoff (starting at 15s).
    // Throws on timeout with a message listing missing files.
    //   exists : closure taking a full path string and returning true if the file exists.
    //            Callers typically pass `{ p -> file(p).exists() }` so S3 paths work via Nextflow's file() API.
    static void waitForFiles(List<String> expected, String outputDir, long maxWaitMins,
                              Closure<Boolean> exists) {
        if (maxWaitMins < 0) {
            throw new IllegalArgumentException("max_wait_mins must be >= 0, got ${maxWaitMins}")
        }
        def maxWaitMs = maxWaitMins * 60 * 1000
        def intervalMs = 15000L
        def totalWaitedMs = 0L
        def missing = expected.findAll { !exists.call("${outputDir}/${it}") }
        while (!missing.isEmpty()) {
            if (totalWaitedMs >= maxWaitMs) {
                throw new RuntimeException(
                    "Timed out after ${maxWaitMins} minutes waiting for " +
                    "${missing.size()} published output file(s):\n  " +
                    missing.join("\n  "))
            }
            Thread.sleep(intervalMs)
            totalWaitedMs += intervalMs
            intervalMs = intervalMs * 2
            missing = expected.findAll { !exists.call("${outputDir}/${it}") }
        }
    }

    // Current UTC timestamp in the sentinel format used across workflows.
    static String nowUtc() {
        return new Date().format("yyyy-MM-dd HH:mm:ss z (Z)", TimeZone.getTimeZone("UTC"))
    }
}
