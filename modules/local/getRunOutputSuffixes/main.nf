// Extract per-sample output file suffixes from pyproject.toml
process GET_RUN_OUTPUT_SUFFIXES {
    label "python"
    label "single"
    input:
        path(pyproject)
    output:
        env(SUFFIXES), emit: suffixes
    shell:
        '''
        SUFFIXES=$(get_run_output_suffixes.py !{pyproject} | tr '\\n' ',' | sed 's/,$//')
        '''
}
