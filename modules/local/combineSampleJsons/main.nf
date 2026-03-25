// Combine per-sample JSON files into a single per-group JSON
process COMBINE_SAMPLE_JSONS {
    label "python"
    label "single"
    input:
        tuple val(group), path(json_files)
        val(suffix)
    output:
        tuple val(group), path("${group}_combined.json"), emit: output
    script:
        """
        combine_sample_jsons.py --group ${group} --suffix ${suffix} \
            --output ${group}_combined.json ${json_files}
        """
}
