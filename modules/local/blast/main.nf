// BLASTN (streamed version)
process BLASTN {
    label "BLAST"
    label "blast_resources"
    input:
        tuple val(sample), path(fasta) // Gzipped or plaintext interleaved or single-end FASTA
        val(blast_db_dir)
        val(params_map) // blast_db_prefix, blast_perc_id, blast_qcov_hsp_perc, db_download_timeout)
    output:
        tuple val(sample), path("${sample}_hits.blast.gz"), emit: output
        tuple val(sample), path("${sample}_in.fasta.gz"), emit: input
    script:
        def extractCmd = fasta.toString().endsWith(".gz") ? "zcat" : "cat"
        def inputCmd = fasta.toString().endsWith(".gz") ? "ln -s ${fasta} ${sample}_in.fasta.gz" : "gzip -c ${fasta} > ${sample}_in.fasta.gz"
        """
        # Download BLAST database if not already present
        db_local_path=\$(download_db.py "${blast_db_dir}" "${params_map.db_download_timeout}")
        # Set up command
        io="-db \${db_local_path}/${params_map.blast_db_prefix}"
        par="-perc_identity ${params_map.blast_perc_id} -max_hsps 5 -num_alignments 250 -qcov_hsp_perc ${params_map.blast_qcov_hsp_perc} -num_threads ${task.cpus}"
        fmt="6 qseqid sseqid sgi staxid qlen evalue bitscore qcovs length pident mismatch gapopen sstrand qstart qend sstart send"
        # Run BLAST
        ${extractCmd} ${fasta} | blastn \${io} \${par} -outfmt "\${fmt}" \\
            | gzip > ${sample}_hits.blast.gz
        # Link input to output for testing
        ${inputCmd}
        """
}
