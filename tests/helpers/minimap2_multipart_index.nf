// Test-only helper for tests/modules/local/minimap2/align_split_index.nf.test.
//
// MINIMAP2_INDEX builds with minimap2's default index block size (~4 Gbp), which
// no test-scale reference can exceed, so a tiny index is always single-part. The
// only reason MINIMAP2_SPLIT_INDEX exists is to handle a reference that *does*
// exceed it: minimap2 then re-reads the query once per block and needs
// --split-prefix to merge the per-block output. A low -I forces several blocks
// from a small reference so that merge path is actually exercised.
//
// Note the reference must hold several records: minimap2 splits on sequence
// boundaries, so no -I will split a single-sequence FASTA.

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
        # Assert the index really is multi-part, so the test cannot silently
        # degrade to single-part coverage if minimap2 changes how it splits.
        # Aligning without --split-prefix makes minimap2 warn about the
        # multi-part index (it warns rather than failing, so don't gate on the
        # exit status). Log-line counts are not usable here: they track query
        # minibatches, not index blocks.
        minimap2 -a mm2-multipart-index/mm2_index.mmi combined.fasta > /dev/null 2> err.txt || true
        grep -qiE "multi-part|split-prefix" err.txt || {
            echo "ERROR: index built at -I ${block_size} is single-part" >&2
            cat err.txt >&2
            exit 1
        }
        """
}
