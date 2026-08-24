// Test-only helper for tests/modules/local/minimap2/align_split_index.nf.test.
//
// MINIMAP2_INDEX builds with minimap2's default index block size, so set -I low
// to create several blocks from a small reference to exercise the merge path.
//
// Note the reference must hold more than one record to generate a multi-part index.

process MINIMAP2_INDEX_MULTIPART {
    label "minimap2_samtools"
    label "single"
    tag "id=index,block_size=${block_size}"
    input:
        path(reference_fastas)
        val(block_size)
    output:
        path("mm2-multipart-index")
    script:
        """
        set -euo pipefail
        mkdir mm2-multipart-index
        # Concatenate the inputs and give every record a unique name: the toy
        # FASTAs reuse names, which samtools rejects as a duplicate SAM header.
        cat ${reference_fastas} | awk '/^>/{n++; print ">seq" n; next} {print}' > combined.fasta
        minimap2 -x lr:hq -I ${block_size} -d mm2-multipart-index/mm2_index.mmi combined.fasta
        # Count the blocks minimap2 actually built, so the test cannot silently
        # degrade to single-part coverage. Loading the index against an empty
        # query logs one line per block and does no alignment.
        n_blocks=\$(minimap2 mm2-multipart-index/mm2_index.mmi /dev/null 2>&1 \\
            | grep -c "loaded/built the index")
        [ "\${n_blocks}" -ge 2 ] || {
            echo "ERROR: index built at -I ${block_size} has \${n_blocks} block(s), expected >= 2" >&2
            exit 1
        }
        """
}
