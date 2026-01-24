// Check that pipeline and index versions are compatible

/**********************
| AUXILIARY FUNCTIONS |
**********************/

def isVersionLess(version1, version2) {
    /* Compare two semantic versions and return a boolean stating
    whether the first is less (i.e. older) than the second. */
    // Remove terminal tags (anything after a hyphen)
    def cleanVersion1 = version1.tokenize("-")[0]
    def cleanVersion2 = version2.tokenize("-")[0]
    // Split components by periods
    def v1Components = cleanVersion1.tokenize(".")
    def v2Components = cleanVersion2.tokenize(".")
    // Convert to integers (and raise error if unable)
    def v1IntComponents
    def v2IntComponents
    try {
        v1IntComponents = v1Components.collect{ it.toInteger() }
    } catch (NumberFormatException _e) {
        def msg1 = "Invalid version format: version 1 (${version1}) contains non-integer components."
        throw new IllegalArgumentException(msg1)
    }
    try {
        v2IntComponents = v2Components.collect{ it.toInteger() }
    } catch (NumberFormatException _e) {
        def msg2 = "Invalid version format: version 2 (${version2}) contains non-integer components."
        throw new IllegalArgumentException(msg2)
    }
    // Get the longest version length and pad shorter version with zeros
    def maxLength = Math.max(v1IntComponents.size(), v2IntComponents.size())
    def paddedV1 = v1IntComponents + [0] * (maxLength - v1IntComponents.size())
    def paddedV2 = v2IntComponents + [0] * (maxLength - v2IntComponents.size())

    // Find first differing component
    def diff = (0..<maxLength).find { i -> paddedV1[i] != paddedV2[i] }
    return diff != null ? paddedV1[diff] < paddedV2[diff] : false
}

/***********
| PROCESS |
***********/

process CHECK_VERSIONS {
    label "single"
    label "coreutils"
    input:
        val pipeline_version
        val index_version
        val pipeline_min_index
        val index_min_pipeline
    output:
        val true
    exec:
        if (index_min_pipeline && isVersionLess(pipeline_version, index_min_pipeline)) {
            def msg_a = "Pipeline version is older than index minimum: ${pipeline_version} < ${index_min_pipeline}"
            throw new Exception(msg_a)
        }
        if (pipeline_min_index && isVersionLess(index_version, pipeline_min_index)) {
            def msg_b = "Index version is older than pipeline minimum: ${index_version} < ${pipeline_min_index}"
            throw new Exception(msg_b)
        }
}
