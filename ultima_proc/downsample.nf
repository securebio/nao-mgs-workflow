#!/usr/bin/env nextflow

/*
 * Downsample Illumina fastqs to match Ultima read counts with zero cross-sample overlap.
 *
 * Input: a samplesheet CSV produced by prepare_downsample.py with columns:
 *   output_id, illumina_r1, illumina_r2, target_reads, total_illumina_pairs, fraction, seed
 *
 * For each source prefix (barcoded/NA pair), downsamples once to get the total reads
 * needed, shuffles to eliminate order bias, then partitions sequentially to ensure
 * zero overlap between the paired samples. Processes each lane individually to avoid
 * staging large files.
 *
 * Usage:
 *   nextflow run ultima_proc/downsample.nf -c ultima_proc/downsample.config -profile standard
 */

nextflow.enable.dsl = 2

process DOWNSAMPLE_PAIRED_LANES {
    tag "${source_prefix}:${r1.name}"
    label 'seqkit'
    cpus 2
    memory '8 GB'

    input:
    tuple val(source_prefix), val(barcoded_id), val(na_id), val(barcoded_reads), val(na_reads), val(total_fraction), val(seed), path(r1), path(r2)

    output:
    tuple val(barcoded_id), path("barcoded_${r1}"), path("barcoded_${r2}")
    tuple val(na_id), path("na_${r1}"), path("na_${r2}")

    script:
    def total_reads = barcoded_reads + na_reads
    def barcoded_lines = barcoded_reads * 4
    def na_lines_start = barcoded_lines + 1
    """
    # Downsample to total reads needed for both samples
    seqtk sample -s${seed} ${r1} ${total_fraction} > temp_r1.fastq
    seqtk sample -s${seed} ${r2} ${total_fraction} > temp_r2.fastq

    # Shuffle to eliminate order bias
    seqkit shuffle -s${seed} temp_r1.fastq > shuffled_r1.fastq
    seqkit shuffle -s${seed} temp_r2.fastq > shuffled_r2.fastq

    # Split sequentially: first N reads to barcoded, remaining to NA
    head -n ${barcoded_lines} shuffled_r1.fastq | gzip -1 > barcoded_${r1}
    head -n ${barcoded_lines} shuffled_r2.fastq | gzip -1 > barcoded_${r2}

    tail -n +${na_lines_start} shuffled_r1.fastq | gzip -1 > na_${r1}
    tail -n +${na_lines_start} shuffled_r2.fastq | gzip -1 > na_${r2}

    # Cleanup temp files
    rm temp_r1.fastq temp_r2.fastq shuffled_r1.fastq shuffled_r2.fastq
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
    // Helper function to extract source prefix from output_id
    def extractSourcePrefix = { output_id ->
        def name = output_id.replace("_illumina_matched", "")
        def parts = name.split("-")
        def prefix_parts = []
        for (part in parts) {
            if (part ==~ /^[ACGT]{10}$/ || part == "NA") {
                break
            }
            prefix_parts.add(part)
        }
        return prefix_parts.join("-")
    }

    def isBarcoded = { output_id -> !output_id.contains("-NA-") }

    // Parse samplesheet and group by source prefix to find barcoded/NA pairs
    Channel
        .fromPath(params.samplesheet)
        .splitCsv(header: true)
        .map { row ->
            def source_prefix = extractSourcePrefix(row.output_id)
            tuple(source_prefix, row)
        }
        .groupTuple()
        .map { source_prefix, rows ->
            // Find barcoded and NA samples in this group
            def barcoded_row = null
            def na_row = null
            rows.each { row ->
                if (isBarcoded(row.output_id)) {
                    barcoded_row = row
                } else {
                    na_row = row
                }
            }
            if (barcoded_row && na_row) {
                tuple(source_prefix, barcoded_row, na_row)
            } else {
                log.warn "Unpaired samples for prefix ${source_prefix}: ${rows.collect { it.output_id }}"
                null
            }
        }
        .filter { it != null }
        .flatMap { source_prefix, barcoded_row, na_row ->
            // Calculate total reads and fraction needed
            def barcoded_reads = barcoded_row.target_reads as Integer
            def na_reads = na_row.target_reads as Integer
            def total_reads = barcoded_reads + na_reads
            def total_illumina_pairs = barcoded_row.total_illumina_pairs as Integer
            def total_fraction = (total_reads as Double) / total_illumina_pairs
            def seed = 42  // Use fixed seed for reproducibility

            // Explode into per-lane tuples
            def r1_files = barcoded_row.illumina_r1.tokenize(';')
            def r2_files = barcoded_row.illumina_r2.tokenize(';')

            [r1_files, r2_files].transpose().collect { r1, r2 ->
                tuple(source_prefix, barcoded_row.output_id, na_row.output_id,
                      barcoded_reads, na_reads, total_fraction, seed, file(r1), file(r2))
            }
        }
        .set { paired_lanes_ch }

    // Process paired downsampling for each lane
    DOWNSAMPLE_PAIRED_LANES(paired_lanes_ch)

    // Separate outputs and group by sample ID for concatenation
    def barcoded_ch = DOWNSAMPLE_PAIRED_LANES.out[0]
        .groupTuple()
        .map { output_id, r1_list, r2_list ->
            tuple(output_id, r1_list.sort(), r2_list.sort())
        }

    def na_ch = DOWNSAMPLE_PAIRED_LANES.out[1]
        .groupTuple()
        .map { output_id, r1_list, r2_list ->
            tuple(output_id, r1_list.sort(), r2_list.sort())
        }

    // Concatenate both barcoded and NA samples
    CAT_DOWNSAMPLED(barcoded_ch.mix(na_ch))
}
