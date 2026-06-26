// Validate that all expected output files for a (whole-workflow or per-group) run
// have been published, then write a JSON completion marker with start/completion
// timestamps. A single parametrized module serves every caller: the caller
// supplies the expected-outputs key(s), the wildcard to expand and its values,
// the marker filename, the process tag id, and the timestamp field-name prefix.
//
// Uses exec: to run on the head node so file().exists() works natively against S3.
// The shared regex/poll/timestamp helpers in lib/SentinelUtils.groovy are loaded
// explicitly from params_map.sentinel_utils_path rather than via Nextflow's lib/
// autoload, so the helper resolves regardless of which project is the Nextflow
// entrypoint (autoload only covers the top-level projectDir/lib).
process WRITE_SENTINEL {
    executor 'local'
    label "sentinel"
    tag "id=${tag_id}"
    input:
        val(ready)                                                 // Dependency signal: collected items from all output channels
        tuple val(tag_id), val(marker_name), val(wildcard_values)  // Per-instance: process tag id, output filename, wildcard substitutions
        val(expected_keys)                                         // expected-outputs-* suffixes to validate (caller resolves any platform logic)
        val(wildcard)                                              // Placeholder expanded in patterns (e.g. "SAMPLE" or "GROUP")
        val(start_time)                                            // Start time string
        val(schema_prefix)                                         // Timestamp field-name prefix ("run" -> runStartedAt/runCompletedAt; "" -> startedAt/completedAt)
        val(params_map)                                            // Workflow params (+ output_dir, pyproject_path, sentinel_utils_path injected by caller)
    output:
        path("*sentinel.json"), emit: sentinel
    exec:
        // Parsed once per invocation (per group for the DOWNSTREAM fan-out). This is
        // cheap head-node-only work, bounded by the sentinel `maxForks` cap, and dwarfed
        // by the S3 output polling below; the explicit per-task load is what keeps the
        // module independent of lib/ autoload, so re-parsing is an accepted trade-off.
        def sentinel_utils = new GroovyClassLoader().parseClass(
            file(params_map.sentinel_utils_path).toFile())
        def pyproject_text = file(params_map.pyproject_path).text
        def expected = sentinel_utils.getExpectedOutputs(
            pyproject_text, expected_keys as List<String>, wildcard as String, wildcard_values as List<String>)
        sentinel_utils.waitForFiles(expected, params_map.output_dir as String,
            sentinel_utils.resolveMaxWaitMins(params_map)) { p -> file(p).exists() }
        // A blank schema_prefix yields generic startedAt/completedAt field names; this
        // keeps the module's marker schema reusable by callers that don't want the
        // workflow-prefixed names that run/downstream use.
        def started_field = schema_prefix ? "${schema_prefix}StartedAt" : "startedAt"
        def completed_field = schema_prefix ? "${schema_prefix}CompletedAt" : "completedAt"
        def sentinel_content = [
            (started_field as String): start_time,
            (completed_field as String): sentinel_utils.nowUtc(),
        ]
        task.workDir.resolve(marker_name as String).text =
            groovy.json.JsonOutput.prettyPrint(
                groovy.json.JsonOutput.toJson(sentinel_content)
            ) + "\n"
}
