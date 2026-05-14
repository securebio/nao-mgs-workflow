// Reproducer matrix for CONCATENATE_GENOME_FASTA failures observed in
// s3://nao-jo/workflow-index-test/work/a9/67bd27f2d2b457e950c904fbc9acea/.
//
// Wires the failing task's two upstream S3 inputs (ncbi_genomes/ from
// PREPARE_VIRAL_METADATA and virus-genome-paths.csv) into 2x4 = 8 parallel runs:
//   variants  : with_seqkit (rmdup --by-name) vs. without_seqkit (raw cat)
//   memory_gb : 1, 16, 32, 128
//
// All variants include the `|| true` SIGPIPE fix on the `ls | head` diagnostic
// (the `broken` variant has been dropped — separately confirmed to fail with
// exit 141 from the SIGPIPE+pipefail interaction).
//
// errorStrategy "ignore" + optional outputs so the workflow completes
// regardless; each task's workdir contains .command.{sh,log,err,out},
// .exitcode for inspection, and Batch job metadata records the OOM reason.
//
// Usage:
//   NXF_VER=25.10.5 nextflow run debug/concatenate_repro.nf \
//       -c debug/seqkit_rmdup_oom.config \
//       -profile batch \
//       --base_dir s3://nao-jo/debug/concat-repro/$(date -u +%Y%m%d-%H%M)-ondemand \
//       --queue jo-on-demand-batch-jq

nextflow.enable.dsl=2

params.genome_dir = "s3://nao-jo/workflow-index-test/work/8d/eebd5d78fcabd0f84d704f48f4823b/ncbi_genomes"
params.path_file  = "s3://nao-jo/workflow-index-test/work/e6/66b6d658b62ccd722e0ca9996ff960/virus-genome-paths.csv"

// 2x4 matrix: {with_seqkit, without_seqkit} x {1, 16, 32, 128} GB.
// Each task runs the production-equivalent script with the SIGPIPE fix and
// the given variant's seqkit-rmdup behavior, requesting the given memory.
process CONCATENATE_GENOME_FASTA_REPRO {
    tag "${variant}-${mem_gb}gb"
    label "seqkit"
    cpus 1
    memory "${mem_gb} GB"
    errorStrategy "ignore"
    publishDir "${params.base_dir}/results/${variant}-${mem_gb}gb", mode: "copy"
    input:
        tuple val(variant), val(mem_gb), path(genome_dir), path(path_file)
    output:
        path("genomes.fasta.gz"),       optional: true
        path("genomes-duplicates.tsv"), optional: true
    script:
        // Common preamble: inputs sanity-check + diagnostics. The `|| true`
        // on `ls | head` swallows the SIGPIPE-driven exit 141 when the
        // directory has more entries than `head` reads (separately reproduced).
        def preamble = """\
        set -euo pipefail
        echo "VARIANT=${variant} MEM=${mem_gb}GB"
        echo "Genome directory contains" \$(ls ${genome_dir} | wc -l) "files, beginning with:"
        ls -1 ${genome_dir} | head || true
        if [[ ! -s ${path_file} ]]; then
            echo "No matching files found!"
            exit 1
        fi
        echo "Filepath file contains" \$(cat ${path_file} | wc -l) "paths, beginning with:"
        head ${path_file}
        """.stripIndent()
        if (variant == 'with_seqkit')
            """
            ${preamble}
            xargs cat < ${path_file} \\
                | seqkit rmdup --by-name --threads ${task.cpus} \\
                    -D genomes-duplicates.tsv -o genomes.fasta.gz
            if [[ -s genomes-duplicates.tsv ]]; then
                echo "Duplicate sequence IDs removed:"
                cat genomes-duplicates.tsv
            fi
            echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
            """
        else
            // Raw concat with no dedup. Each input is already gzipped, so
            // concatenating the .gz bytes yields a valid multi-stream gzip
            // that zcat handles natively. Tests the hypothesis that seqkit
            // rmdup is the OOM culprit (vs. e.g. the cat / output gzip step).
            """
            ${preamble}
            xargs cat < ${path_file} > genomes.fasta.gz
            echo "Output file contains" \$(zcat genomes.fasta.gz | grep -c '^>') "sequences."
            """
}

workflow {
    if (!params.base_dir || params.base_dir == "BASE_DIR_PATH") {
        error "Must supply --base_dir (S3 path)"
    }
    genome_dir = Channel.fromPath(params.genome_dir, type: 'dir', checkIfExists: true)
    path_file  = Channel.fromPath(params.path_file,                  checkIfExists: true)
    Channel.of('with_seqkit', 'without_seqkit')
        .combine(Channel.of(1, 16, 32, 128))
        .combine(genome_dir)
        .combine(path_file)
        .set { repro_inputs }
    CONCATENATE_GENOME_FASTA_REPRO(repro_inputs)
}
