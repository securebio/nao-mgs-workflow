// Validate that all expected DOWNSTREAM output files for a group have been published,
// then write a per-group {group}_sentinel.json completion marker with timestamps.
// Runs once per group; uses exec: on the head node for native S3 support via file().exists().
// Shared regex/poll/timestamp helpers live in lib/SentinelUtils.groovy.
// Note: if the per-group fan-out channel is empty (e.g. a groups TSV with only a header),
// this process does not run and no sentinel is produced.
process WRITE_SENTINEL_DOWNSTREAM {
    executor 'local'
    label "sentinel"
    tag "id=${group}"
    input:
        val(group)                     // Group name; drives per-group fan-out
        val(ready)                     // Dependency signal: collected items from all downstream publish channels
        val(downstream_start_time)     // DOWNSTREAM start time string
        val(params_map)                // Workflow params (+ output_dir and pyproject_path injected by caller)
    output:
        path("${group}_sentinel.json"), emit: sentinel
    exec:
        def pyprojectText = file(params_map.pyproject_path).text
        def wfKey = params_map.platform == "ont" ? "downstream-ont" : "downstream"
        def expected = SentinelUtils.getExpectedOutputs(pyprojectText, [wfKey], "GROUP", [group as String])
        SentinelUtils.waitForFiles(expected, params_map.output_dir as String, SentinelUtils.resolveMaxWaitMins(params_map)) { p -> file(p).exists() }
        def sentinelContent = [
            downstreamStartedAt: downstream_start_time,
            downstreamCompletedAt: SentinelUtils.nowUtc()
        ]
        task.workDir.resolve("${group}_sentinel.json").text =
            groovy.json.JsonOutput.prettyPrint(
                groovy.json.JsonOutput.toJson(sentinelContent)
            ) + "\n"
}
