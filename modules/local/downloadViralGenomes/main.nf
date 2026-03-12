// Download viral genomes for a single taxon using NCBI datasets CLI
process DOWNLOAD_VIRAL_GENOMES {
    label "ncbi_datasets"
    label "small"
    input:
        val(taxid)
        val(assembly_source)
        val(extra_args)
    output:
        path("genomes/*.fna.gz"), emit: genomes
        path("metadata.tsv"), emit: metadata
    script:
        """
        datasets download genome taxon ${taxid} \\
            --assembly-source ${assembly_source} \\
            --include genome \\
            --no-progressbar \\
            ${extra_args} \\
            --filename output.zip

        unzip -o output.zip -d output/

        # Convert assembly report to TSV with standardized column names
        dataformat tsv genome \\
            --inputfile output/ncbi_dataset/data/assembly_data_report.jsonl \\
            --fields accession,organism-tax-id,organism-name,source_database \\
            > raw_metadata.tsv

        # Replace header with standardized column names
        { echo -e "assembly_accession\\ttaxid\\torganism_name\\tsource_database"
          tail -n +2 raw_metadata.tsv
        } > metadata.tsv

        # Flatten genome FASTAs into genomes/ directory
        mkdir -p genomes
        find output/ncbi_dataset/data -name '*.fna' -exec gzip {} \\;
        find output/ncbi_dataset/data -name '*.fna.gz' -exec mv {} genomes/ \\;

        rm -rf output/ output.zip raw_metadata.tsv
        echo "Downloaded \$((  \$(wc -l < metadata.tsv) - 1  )) assemblies for taxid ${taxid}"
        """
}
