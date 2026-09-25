// Shared helpers for the WRITE_SENTINEL_RUN and WRITE_SENTINEL_DOWNSTREAM modules.
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
        List<String> expected = []
        def placeholder = "{${wildcard}}"
        for (k in keys) {
            def fullKey = "expected-outputs-${k}"
            def quotedKey = java.util.regex.Pattern.quote(fullKey)
            def sectionMatch = (pyprojectText =~ /(?s)${quotedKey} = \[(.*?)\]/)
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
        // unique() is defensive against accidental duplicates in expected-outputs-* arrays
        return expected.sort().unique()
    }

    // Expected outputs whose file name no upstream task emitted. A failed task emits nothing,
    // so these will never be published and there is no point polling for them.
    //   emitted : the values collected from the publish channels, nested in any way
    // Emitted files are matched by name, since their publish directory isn't known here, so
    // expected names must be unique. A match only skips this fast check: waitForFiles still
    // checks each exact published path. Non-Path values (e.g. sample names) are ignored.
    static List<String> neverEmitted(List<String> expected, Collection emitted) {
        def repeated = expected.collect { it.tokenize("/").last() }
            .countBy { it }.findAll { name, n -> n > 1 }.keySet()
        if (!repeated.isEmpty()) {
            throw new IllegalStateException(
                "Expected outputs share file names, so emitted files can't be matched to them: " +
                repeated.sort().join(", "))
        }
        def names = emitted.flatten()
            .findAll { it instanceof java.nio.file.Path }
            .collect { it.fileName.toString() } as Set
        return expected.findAll { !names.contains(it.tokenize("/").last()) }
    }

    // Throw if any expected output was never emitted, naming the missing files.
    static void checkEmitted(List<String> expected, Collection emitted) {
        def missing = neverEmitted(expected, emitted)
        if (!missing.isEmpty()) {
            throw new RuntimeException(
                "${missing.size()}/${expected.size()} expected output file(s) were never emitted, " +
                "so an upstream task failed:\n  " + missing.join("\n  "))
        }
    }

    // Poll outputDir for each expected file with exponential backoff starting at 15s
    // (each interval doubles, and the last is cut short so the total wait never exceeds
    // maxWaitMins, the timeout).
    // Throws on timeout with a message listing missing files.
    //   exists : closure taking a full path string and returning true if the file exists.
    //            Callers pass `{ p -> file(p).exists() }` so S3 paths work via Nextflow's file() API.
    static void waitForFiles(List<String> expected, String outputDir, long maxWaitMins,
                              Closure<Boolean> exists) {
        if (maxWaitMins < 0) {
            throw new IllegalArgumentException("max_wait_mins must be >= 0, got ${maxWaitMins}")
        }
        def timeoutMs = maxWaitMins * 60 * 1000
        def intervalMs = 15000L
        def totalWaitedMs = 0L
        def missing = expected.findAll { !exists.call("${outputDir}/${it}") }
        while (!missing.isEmpty()) {
            if (totalWaitedMs >= timeoutMs) {
                throw new RuntimeException(
                    "Timed out after ${maxWaitMins} minutes waiting for " +
                    "${missing.size()}/${expected.size()} remaining published output file(s) " +
                    "(polled for ${totalWaitedMs.intdiv(1000)}s):\n  " +
                    missing.join("\n  "))
            }
            def sleepMs = Math.min(intervalMs, timeoutMs - totalWaitedMs)
            Thread.sleep(sleepMs)
            totalWaitedMs += sleepMs
            intervalMs = intervalMs * 2
            missing = expected.findAll { !exists.call("${outputDir}/${it}") }
        }
    }

    // Resolve params.sentinel_max_wait_mins to a long, falling back to 32 if unset.
    static long resolveMaxWaitMins(Object params) {
        return params.sentinel_max_wait_mins != null ? params.sentinel_max_wait_mins as long : 32L
    }

    // Current UTC timestamp in the sentinel format used across workflows.
    static String nowUtc() {
        return new Date().format("yyyy-MM-dd HH:mm:ss z (Z)", TimeZone.getTimeZone("UTC"))
    }
}
