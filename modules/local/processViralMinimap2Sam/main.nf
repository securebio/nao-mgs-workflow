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
        # Decompress once, then split: headers (@-lines) then sorted alignments lines.
        # Argument notes:
        #   || true: grep returns exit code 1 when no lines match (e.g. empty SAM)
        #   LC_ALL=C: sort by raw byte value so the order matches Python's string comparison
        #   -S 1G caps sort's memory usage; it spills to disk if needed.
        zcat ${virus_sam} > raw.sam
        grep '^@' raw.sam > sorted.sam
        { grep -v '^@' raw.sam || true; } | LC_ALL=C sort -t$'\t' -k1,1 -S 1G >> sorted.sam
        rm raw.sam

        # Sort FASTQ by read id.
        # 4-line records are combined into one tap-separated line, sorted with C locale,
        # and then split back into 4-line records.
        zcat ${clean_reads} \
            | paste - - - - \
            | LC_ALL=C sort -k1,1 -S 1G \
            | tr '\t' '\n' \
            > sorted.fastq

        # Run Python script on sorted inputs
        process_viral_minimap2_sam.py -a sorted.sam -r sorted.fastq \
            -m ${metadata} -v ${virus_db} -o ${out}

        # Link input to output for testing
        ln -s !{virus_sam} input_!{virus_sam}
        '''
}
