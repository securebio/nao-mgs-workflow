// Validate that all expected RUN output files have been published,
// then write a sentinel.json completion marker with timestamps.
// Uses exec: to run on the head node for native S3 support via file().exists().
process WRITE_SENTINEL {
    label "single"
    input:
        val(ready)           // Dependency signal: collected items from all output channels
        val(sample_names)    // List of sample names from samplesheet
        val(start_time)      // Start time string
        val(config)          // Map: output_dir, pyproject_path, platform, max_wait_mins
    output:
        path("sentinel.json"), emit: sentinel
    exec:
        // Parse expected output patterns from pyproject.toml
        // Uses the same regex approach as the test helper getExpectedOutputs
        def pyprojectText = file(config.pyproject_path).text
        def keys = ["run"]
        if (config.platform == "illumina") keys.add("run-shortread-extra")
        def expected = []
        for (wfKey in keys) {
            def key = "expected-outputs-${wfKey}"
            def sectionMatch = (pyprojectText =~ /(?s)${key} = \[(.*?)\]/)
            if (sectionMatch) {
                def patterns = (sectionMatch[0][1] =~ /"([^"]+)"/).collect { it[1] }
                for (pattern in patterns) {
                    if (pattern.contains("{SAMPLE}")) {
                        for (sample in sample_names) {
                            expected.add(pattern.replace("{SAMPLE}", sample))
                        }
                    } else {
                        expected.add(pattern)
                    }
                }
            }
        }
        expected = expected.sort()
        // Check files exist in output directory with exponential backoff
        // Handles both local and S3 paths via Nextflow's file() API
        def maxWaitMs = (config.max_wait_mins as long) * 60 * 1000
        def intervalMs = 15000L
        def totalWaitedMs = 0L
        def missing = expected.findAll { !file("${config.output_dir}/${it}").exists() }
        while (!missing.isEmpty()) {
            if (totalWaitedMs >= maxWaitMs) {
                error("Timed out after ${config.max_wait_mins} minutes waiting for " +
                      "${missing.size()} published output file(s):\n  " +
                      missing.join("\n  "))
            }
            sleep(intervalMs)
            totalWaitedMs += intervalMs
            intervalMs = intervalMs * 2
            missing = expected.findAll { !file("${config.output_dir}/${it}").exists() }
        }
        // If file check completes without error, write sentinel.json
        def sentinel = [
            runStartedAt: start_time,
            runCompletedAt: new java.util.Date().format("yyyy-MM-dd HH:mm:ss z (Z)")
        ]
        task.workDir.resolve("sentinel.json").text =
            groovy.json.JsonOutput.prettyPrint(
                groovy.json.JsonOutput.toJson(sentinel)
            ) + "\n"
}
