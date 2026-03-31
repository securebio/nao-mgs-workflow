#!/usr/bin/env nextflow

// Standalone workflow to generate simulated paired-end reads from single-end Ultima data.
// For each sample, produces R1 (copy of original) and R2 (reverse complement).
// Also generates a paired-end samplesheet for use with the main mgs-workflow pipeline.
//
// Usage:
//   nextflow run ultima_proc/simulate_pe.nf -c ultima_proc/simulate_pe.config -profile standard
//
// Input samplesheet format (single-end):
//   sample,fastq
//   sample_01,s3://bucket/sample_01.fastq.gz
//
// Output: R1/R2 FASTQs in params.output_dir, plus samplesheet_pe.csv

include { SIMULATE_PE } from './modules/simulate_pe'

workflow {
    // Parse single-end samplesheet
    samplesheet = Channel
        .fromPath(params.sample_sheet)
        .splitCsv(header: true)
        .map { row -> tuple(row.sample, file(row.fastq)) }

    // Generate R1 (copy) and R2 (revcomp) for each sample
    SIMULATE_PE(samplesheet)

    // Generate paired-end samplesheet pointing to published output files
    SIMULATE_PE.out.reads
        .map { sample, r1, r2 ->
            "${sample},${params.output_dir}/${sample}_R1.fastq.gz,${params.output_dir}/${sample}_R2.fastq.gz"
        }
        .collectFile(
            name: 'samplesheet_pe.csv',
            storeDir: "${params.output_dir}",
            seed: 'sample,fastq_1,fastq_2',
            newLine: true
        )
}
