// Create empty output files for a group with no virus hits
process CREATE_EMPTY_GROUP_OUTPUTS {
    label "python"
    label "single"
    tag "id=${group}"
    input:
        val(group)
        path(pyproject_toml)
        path(schema_dir)
        val(platform)
        val(pattern_filter)
    output:
        tuple val(group), path("${group}_*.tsv.gz"), emit: outputs
        path("input_${pyproject_toml}"), emit: pyproject
        path("input_schemas"), emit: schemas
    script:
        def opts = ["--platform ${platform == 'ont' ? 'ont' : 'illumina'}"]
        if (pattern_filter) opts << "--pattern-filter '${pattern_filter}'"
        def opts_str = opts.join(' ')
        """
        create_empty_group_outputs.py "${group}" ${pyproject_toml} ${opts_str} --schema-dir ${schema_dir}
        ln -s ${pyproject_toml} input_${pyproject_toml}
        ln -s ${schema_dir} input_schemas
        """
}
