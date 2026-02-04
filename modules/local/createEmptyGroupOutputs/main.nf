// Create empty output files for groups with no virus hits
process CREATE_EMPTY_GROUP_OUTPUTS {
    label "python"
    label "single"
    input:
        path(empty_groups_tsv)
        path(pyproject_toml)
        path(schema_dir)
        val(platform)
    output:
        path("*_*.tsv.gz"), emit: outputs, optional: true
    script:
        def platform_arg = platform == "ont" ? "--platform ont" : "--platform illumina"
        """
        create_empty_group_outputs.py ${empty_groups_tsv} ${pyproject_toml} ./ ${platform_arg} --schema-dir ${schema_dir}
        """
}
