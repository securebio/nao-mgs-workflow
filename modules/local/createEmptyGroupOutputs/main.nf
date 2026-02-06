// Create empty output files for groups with no virus hits
process CREATE_EMPTY_GROUP_OUTPUTS {
    label "python"
    label "single"
    input:
        val(missing_groups)
        path(pyproject_toml)
        path(schema_dir)
        val(platform)
        val(pattern_filter)
    output:
        path("*_*.tsv.gz"), emit: outputs, optional: true
    script:
        def groups_arg = missing_groups.join(',')
        def opts = ["--platform ${platform == 'ont' ? 'ont' : 'illumina'}"]
        if (pattern_filter) opts << "--pattern-filter ${pattern_filter}"
        def opts_str = opts.join(' ')
        """
        create_empty_group_outputs.py "${groups_arg}" ${pyproject_toml} ${opts_str} --schema-dir ${schema_dir}
        """
}
