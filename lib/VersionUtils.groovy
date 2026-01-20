import groovy.toml.TomlSlurper

/**
 * Utility class for reading version information from pyproject.toml files.
 * Nextflow automatically loads classes from the lib/ directory.
 */
class VersionUtils {
    /**
     * Read version information from a pyproject.toml file.
     * Works for both local pyproject.toml and index pyproject.toml from S3.
     *
     * @param pyprojectPath Path to the pyproject.toml file
     * @return Map with keys: pipeline, indexMinPipeline, pipelineMinIndex
     */
    static Map readVersions(Object pyprojectPath) {
        def toml = new TomlSlurper().parse(new File(pyprojectPath.toString()))
        return [
            pipeline: toml.project.version,
            indexMinPipeline: toml.tool?.'mgs-workflow'?.'index-min-pipeline-version',
            pipelineMinIndex: toml.tool?.'mgs-workflow'?.'pipeline-min-index-version'
        ]
    }
}
