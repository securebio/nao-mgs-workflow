// Validate that all expected RUN output files have been published,
// then write a sentinel.json completion marker with timestamps.
// Uses exec: to run on the head node for native S3 support via file().exists().
// Shared regex/poll/timestamp helpers live in lib/SentinelUtils.groovy.
process WRITE_SENTINEL_RUN {
    executor 'local'
    input:
        val(ready)           // Dependency signal: collected items from all output channels
        val(sample_names)    // List of sample names from samplesheet
        val(start_time)      // Start time string
        val(params_map)      // Workflow params (+ output_dir and pyproject_path injected by caller)
    output:
        path("sentinel.json"), emit: sentinel
    exec:
        def pyprojectText = file(params_map.pyproject_path).text
        def keys = ["run"]
        if (params_map.platform == "illumina") keys.add("run-shortread-extra")
        def expected = SentinelUtils.getExpectedOutputs(pyprojectText, keys, "SAMPLE", sample_names as List<String>)
        SentinelUtils.waitForFiles(expected, params_map.output_dir as String, SentinelUtils.resolveMaxWaitMins(params_map)) { p -> file(p).exists() }
        def sentinelContent = [
            runStartedAt: start_time,
            runCompletedAt: SentinelUtils.nowUtc()
        ]
        task.workDir.resolve("sentinel.json").text =
            groovy.json.JsonOutput.prettyPrint(
                groovy.json.JsonOutput.toJson(sentinelContent)
            ) + "\n"
}
