#!/usr/bin/env nextflow

/*
 * Downsample Illumina fastqs to match Ultima read counts.
 *
 * Input: a samplesheet CSV produced by prepare_downsample.py with columns:
 *   output_id, illumina_r1, illumina_r2, target_reads, total_illumina_pairs, fraction, seed
 *
 * For each row, downsamples each lane-split fastq individually (same fraction
 * and seed), then concatenates the small downsampled outputs. This avoids
 * staging/catting tens of GB of raw data before subsampling.
 *
 * Usage:
 *   nextflow run ultima_proc/downsample.nf -c ultima_proc/downsample.config -profile standard
 */

nextflow.enable.dsl = 2

process DOWNSAMPLE_LANE {
    tag "${output_id}:${r1.name}"
    label 'seqtk'
    cpus 2
    memory '4 GB'

    input:
    tuple val(output_id), path(r1), path(r2), val(fraction), val(seed)

    output:
    tuple val(output_id), path("ds_${r1}"), path("ds_${r2}")

    script:
    """
    seqtk sample -s${seed} ${r1} ${fraction} | gzip -1 > ds_${r1}
    seqtk sample -s${seed} ${r2} ${fraction} | gzip -1 > ds_${r2}
    """
}

process CAT_DOWNSAMPLED {
    tag "${output_id}"
    label 'coreutils'
    cpus 1
    memory '2 GB'
    publishDir "${params.outdir}", mode: 'copy'

    input:
    tuple val(output_id), path(r1_files), path(r2_files)

    output:
    tuple val(output_id), path("${output_id}_R1.fastq.gz"), path("${output_id}_R2.fastq.gz")

    script:
    """
    cat ${r1_files} > ${output_id}_R1.fastq.gz
    cat ${r2_files} > ${output_id}_R2.fastq.gz
    """
}

workflow {
    // Parse samplesheet: explode each row into per-lane tuples
    Channel
        .fromPath(params.samplesheet)
        .splitCsv(header: true)
        .flatMap { row ->
            def output_id = row.output_id
            def r1_files  = row.illumina_r1.tokenize(';')
            def r2_files  = row.illumina_r2.tokenize(';')
            def fraction  = row.fraction as Double
            def seed      = row.seed as Integer
            [r1_files, r2_files].transpose().collect { r1, r2 ->
                tuple(output_id, file(r1), file(r2), fraction, seed)
            }
        }
        .set { lanes_ch }

    // Downsample each lane independently
    DOWNSAMPLE_LANE(lanes_ch)

    // Group by output_id and concatenate
    DOWNSAMPLE_LANE.out
        .groupTuple()
        .map { output_id, r1_list, r2_list ->
            tuple(output_id, r1_list.sort(), r2_list.sort())
        }
        .set { grouped_ch }

    CAT_DOWNSAMPLED(grouped_ch)
}
