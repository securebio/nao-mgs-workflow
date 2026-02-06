// Process SAM file (add reference taxid, add clean read information, turn into TSV)
process PROCESS_VIRAL_MINIMAP2_SAM {
    label "pysam_biopython"
    label "single"
    input:
        tuple val(sample), path(virus_sam), path(clean_reads)
        path genbank_metadata_path
        path viral_db_path

    output:
        tuple val(sample), path("${sample}_minimap2_sam_processed.tsv.gz"), emit: output
        tuple val(sample), path("input_${virus_sam}"), emit: input
    shell:
        '''
        set -euo pipefail
        out=!{sample}_minimap2_sam_processed.tsv.gz
        metadata=!{genbank_metadata_path}
        virus_db=!{viral_db_path}
        virus_sam=!{virus_sam}
        clean_reads=!{clean_reads}

        # Sort SAM by read ID, preserving headers for pysam compatibility.
        # Decompress once, then split: headers (@-lines) go first,
        # followed by alignment lines sorted by read ID (first tab field).
        # -S 1G caps sort's memory usage; it spills to disk if needed.
        zcat ${virus_sam} > raw.sam
        grep '^@' raw.sam > sorted.sam
        # || true: grep returns exit code 1 when no lines match (e.g. empty SAM)
        { grep -v '^@' raw.sam || true; } | sort -t$'\t' -k1,1 -S 1G >> sorted.sam
        rm raw.sam

        # Sort FASTQ file by read ID for streaming merge join with SAM.
        # FASTQ records are 4 lines each, so we join each group of 4 lines
        # into one tab-delimited line (paste), sort by the first field (the
        # @read_id header line), then restore the 4-line-per-record format (tr).
        # -S 1G caps sort's memory usage; it spills to disk if needed.
        zcat ${clean_reads} \
            | paste - - - - \
            | sort -k1,1 -S 1G \
            | tr '\t' '\n' \
            > sorted.fastq

        # Run Python script on sorted inputs
        process_viral_minimap2_sam.py -a sorted.sam -r sorted.fastq \
            -m ${metadata} -v ${virus_db} -o ${out}

        # Link input to output for testing
        ln -s !{virus_sam} input_!{virus_sam}
        '''
}
