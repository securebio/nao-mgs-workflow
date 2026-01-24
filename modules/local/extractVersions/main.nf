// Extract version information from pyproject.toml files
process EXTRACT_VERSIONS {
    label "single"
    label "python"
    input:
        path pipeline_pyproject, stageAs: "pipeline_pyproject.toml"
        path index_pyproject, stageAs: "index_pyproject.toml"
    output:
        env PIPELINE_VERSION, emit: pipeline_version
        env INDEX_VERSION, emit: index_version
        env PIPELINE_MIN_INDEX, emit: pipeline_min_index
        env INDEX_MIN_PIPELINE, emit: index_min_pipeline
    script:
        """
        eval \$(extract_versions.py pipeline_pyproject.toml index_pyproject.toml)
        """
}
