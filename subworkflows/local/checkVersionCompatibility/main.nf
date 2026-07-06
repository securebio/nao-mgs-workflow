/**********************************************
| SUBWORKFLOW: VERSION COMPATIBILITY CHECKING |
**********************************************/

/* Takes in a reference/index directory and a pipeline directory,
derives pyproject.toml paths, and raises an error if their versions
are incompatible. Emits both pyproject paths for downstream use. */

include { EXTRACT_VERSIONS } from "../../../modules/local/extractVersions"
include { CHECK_VERSIONS } from "../../../modules/local/checkVersions"

/***********************
| AUXILIARY FUNCTIONS |
***********************/

def getIndexPyprojectPath(ref_dir) {
    /* Get the pyproject.toml path for an index, with backwards compatibility
    for old indexes that use separate version text files. */
    def index_pyproject_file = file("${ref_dir}/logging/pyproject.toml")
    if (index_pyproject_file.exists()) {
        return index_pyproject_file
    }
    // Fall back to old format - generate pyproject content from old files
    def index_version = file("${ref_dir}/logging/pipeline-version.txt").text.trim()
    def index_min_pipeline = file("${ref_dir}/logging/index-min-pipeline-version.txt").text.trim()
    def pyproject_content = """\
[project]
version = "${index_version}"

[tool.mgs-workflow]
index-min-pipeline-version = "${index_min_pipeline}"
"""
    def temp_file = File.createTempFile("index-pyproject", ".toml")
    temp_file.text = pyproject_content
    temp_file.deleteOnExit()
    return file(temp_file.absolutePath)
}

/***********
| WORKFLOW |
***********/

workflow CHECK_VERSION_COMPATIBILITY {
    take:
        ref_dir      // Index directory (contains logging/pyproject.toml)
        project_dir  // Local project directory for the current pipeline execution (contains pyproject.toml)
    main:
        // Derive pyproject.toml paths from directories
        pipeline_pyproject_path = file("${project_dir}/pyproject.toml")
        index_pyproject_path = getIndexPyprojectPath(ref_dir)

        // Extract version info from pyproject.toml files using a process
        // (handles S3 paths correctly by staging files in the container)
        versions_ch = EXTRACT_VERSIONS(pipeline_pyproject_path, index_pyproject_path)

        // Check version compatibility
        CHECK_VERSIONS(
            versions_ch.pipeline_version,
            versions_ch.index_version,
            versions_ch.pipeline_min_index,
            versions_ch.index_min_pipeline
        )
    emit:
        pipeline_pyproject_path = channel.fromPath(pipeline_pyproject_path)
        index_pyproject_path = channel.fromPath(index_pyproject_path)
}
