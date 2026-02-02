// Create empty output files for groups with no virus hits
process CREATE_EMPTY_GROUP_OUTPUTS {
    label "python"
    label "single"
    input:
        val(missing_groups)
        path(pyproject_toml)
        val(platform)
    output:
        path("*_*.tsv.gz"), emit: outputs, optional: true
    script:
        def groups_arg = missing_groups.join(',')
        def platform_arg = platform == "ont" ? "--platform ont" : "--platform illumina"
        """
        create_empty_group_outputs.py "${groups_arg}" ${pyproject_toml} ./ ${platform_arg}
        """
}
