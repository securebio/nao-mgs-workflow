// Concatenate downloaded genomes from ncbi-genome-download according to a file of genome IDs
process CONCATENATE_GENOME_FASTA {
    label "single"
    label "seqtk"
    input:
        path(genome_dir)
        path(path_file)
    output:
        path("genomes.fasta.gz")
    shell:
        '''
        # Diagnostics
        echo "Genome directory contains" $(ls !{genome_dir} | wc -l) "files, beginning with:"
        ls -1 !{genome_dir} | head
        echo "Filepath file contains" $(cat !{path_file} | wc -l) "paths, beginning with:"
        head !{path_file}
        # Concatenate files listed by paths
        if [[ -s !{path_file} ]]; then
            xargs cat < !{path_file} > genomes.fasta.gz
        else
            echo "No matching files found!"
            exit 1
        fi
        echo "Output file contains" $(zcat genomes.fasta.gz | grep "^>" | wc -l) "sequences."
        '''
}
