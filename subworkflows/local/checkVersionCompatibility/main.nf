/**********************************************
| SUBWORKFLOW: VERSION COMPATIBILITY CHECKING |
**********************************************/

/* Takes in pipeline and index pyproject.toml paths and raises an error
if their versions are incompatible. */

include { EXTRACT_VERSIONS } from "../../../modules/local/extractVersions/main.nf"
include { CHECK_VERSIONS } from "../../../modules/local/checkVersions/main.nf"

/***********
| WORKFLOW |
***********/

workflow CHECK_VERSION_COMPATIBILITY {
    take:
        pipeline_pyproject_path  // Local pyproject.toml
        index_pyproject_path     // Index's pyproject.toml from S3
    main:
        // Extract version info from pyproject.toml files using a process
        // (handles S3 paths correctly by staging files in the container)
        EXTRACT_VERSIONS(pipeline_pyproject_path, index_pyproject_path)

        // Check version compatibility
        CHECK_VERSIONS(
            EXTRACT_VERSIONS.out.pipeline_version,
            EXTRACT_VERSIONS.out.index_version,
            EXTRACT_VERSIONS.out.pipeline_min_index,
            EXTRACT_VERSIONS.out.index_min_pipeline
        )
}
