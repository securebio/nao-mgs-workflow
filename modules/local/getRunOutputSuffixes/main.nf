// Extract per-sample output file suffixes from pyproject.toml
process GET_RUN_OUTPUT_SUFFIXES {
    label "python"
    label "single"
    input:
        path(pyproject)
        val(platform)
    output:
        env(SUFFIXES), emit: suffixes
    script:
        """
        SUFFIXES=\$(get_run_output_suffixes.py --platform ${platform} ${pyproject} | tr '\\n' ',' | sed 's/,\$//')
        """
}
