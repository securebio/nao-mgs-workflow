// Build a binary k-mer index from a reference FASTA using nucleaze
// The index is used by NUCLEAZE for k-mer screening
process NUCLEAZE_INDEX {
    label "small"
    label "rust_tools"
    tag "id=index"
    input:
        path(ref_fasta)
        val(k)
        val(index_basename) // basename for the .nucleaze.bin (decouples it from ref_fasta's name)
    output:
        path("*.nucleaze.bin"), emit: index
        path("nucleaze_index.log"), emit: log
    script:
        def index_name = (index_basename ?: ref_fasta.simpleName) + ".nucleaze.bin"
        // nucleaze always reads from stdin; pipe empty input so it exits
        // after building the index instead of waiting for read data
        """
        set -euo pipefail
        echo -n | nucleaze \
            --ref ${ref_fasta} \
            --saveref ${index_name} \
            --k ${k} \
            --canonical \
            --threads ${task.cpus} \
            2>&1 | tee nucleaze_index.log
        """
}
