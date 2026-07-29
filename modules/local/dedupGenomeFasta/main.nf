// Deduplicate the viral genome DB by sequence ID and by sequence.
// Runs after FILTER_GENOME_FASTA so that pattern-excluded records are already
// gone: deduplicating first would let an excluded record win against a clean
// sequence-identical twin, and the filter would then delete the winner,
// dropping a genome for which a legitimate record existed.
// Uses a local scratch directory on Batch profiles as defined in configs/profiles.config.
process DEDUP_GENOME_FASTA {
    label "xsmall"
    label "seqkit"
    label "use_scratch"
    tag "id=index"
    input:
        path(filtered_genomes)
        path(genome_metadata) // Per-genome_id metadata, for the RefSeq/GenBank source of each ID
        val(name_pattern)
    output:
        path("${name_pattern}.fasta.gz")
    script:
        """
        set -euo pipefail
        # Both dedup passes below keep the first occurrence, so record order
        # decides which of a set of duplicates survives. Order RefSeq-derived
        # records ahead of GenBank ones to make RefSeq win: NCBI mints a RefSeq
        # copy of many GenBank viral records, and the copy is usually sequence-
        # identical, so this decides the published genome_id for those genomes.
        # Source comes from the metadata's `source_database` column rather than
        # an accession prefix, because records sourced as NCBI Virus sequences
        # rather than as assemblies are keyed by bare nucleotide accession, with
        # no GCA_/GCF_ to test.
        # Writes two files: RefSeq genome_ids via stdout, and every genome_id to
        # all_ids.txt for the namespace check further down. Rejects any
        # source_database value outside the two known literals, since a renamed
        # enum would otherwise empty refseq_ids.txt and silently resolve every
        # RefSeq/GenBank twin the wrong way.
        # Pre-created so header-only metadata reaches the count guard below
        # rather than dying in `sort` on a file awk never opened.
        : > all_ids.txt
        zcat ${genome_metadata} | awk -F'\\t' '
            NR == 1 {
                for (i = 1; i <= NF; i++) { col[\$i] = i }
                if (!("genome_id" in col)) {
                    print "ERROR: metadata lacks a genome_id column" > "/dev/stderr"
                    exit 1
                }
                if (!("source_database" in col)) {
                    print "ERROR: metadata lacks a source_database column" > "/dev/stderr"
                    exit 1
                }
                gid = col["genome_id"]; src = col["source_database"]
                next
            }
            \$src != "SOURCE_DATABASE_REFSEQ" && \$src != "SOURCE_DATABASE_GENBANK" {
                print "ERROR: unexpected source_database value: " \$src > "/dev/stderr"
                exit 1
            }
            { print \$gid > "all_ids.txt" }
            \$src == "SOURCE_DATABASE_REFSEQ" { print \$gid }
        ' | sort -u > refseq_ids.txt
        sort -u -o all_ids.txt all_ids.txt
        n_metadata_ids=\$(wc -l < all_ids.txt)
        n_refseq_ids=\$(wc -l < refseq_ids.txt)
        # A non-empty filtered FASTA means genomes were downloaded, so metadata
        # with no rows at all means PREPARE_VIRAL_METADATA lost its join. Catch
        # it here: the namespace check below compares against this count and
        # would wave a header-only file straight through.
        if [[ \${n_metadata_ids} -eq 0 ]]; then
            echo "ERROR: metadata contains no genome_id rows" >&2
            exit 1
        fi
        echo "Metadata lists \${n_metadata_ids} genome ID(s), \${n_refseq_ids} of them RefSeq-derived."

        # Partition into RefSeq-first order in a single pass. Pre-create both
        # files so the `cat` below still works when a partition stays empty
        # (e.g. assembly_source = "refseq", where nothing is GenBank-derived).
        : > refseq.fna
        : > genbank.fna
        zcat ${filtered_genomes} \\
            | awk -v refseq_ids=refseq_ids.txt -v all_ids=all_ids.txt '
                BEGIN {
                    while ((getline line < refseq_ids) > 0) { refseq[line] = 1 }
                    while ((getline line < all_ids) > 0) { known[line] = 1 }
                }
                /^>/ {
                    id = substr(\$1, 2)
                    if (id in known) { n_known++ }
                    if (id in refseq) { out = "refseq.fna"; n_refseq++ } else { out = "genbank.fna"; n_genbank++ }
                }
                { print > out }
                END {
                    print n_refseq+0 > "n_refseq.txt"
                    print n_known+0 > "n_known.txt"
                    print "Partitioned " n_refseq+0 " RefSeq and " n_genbank+0 " GenBank record(s)."
                }'
        # IDs absent from the metadata sort with GenBank, so metadata genome_ids
        # and FASTA headers drifting out of the same namespace would silently
        # disable the preference rather than fail. Test that against the whole
        # metadata, not just its RefSeq rows: a build whose RefSeq records were
        # all removed by FILTER_GENOME_FASTA is legitimate, a build where no
        # metadata ID matches any header at all is not.
        # Individually unmapped records are tolerated here rather than being a
        # second enforcement point: FILTER_METADATA_TO_FASTA owns the "every
        # published sequence has a metadata row" invariant and fails the build
        # on it, so a record reaching this step unmapped is caught there.
        if [[ \${n_metadata_ids} -gt 0 && \$(cat n_known.txt) -eq 0 ]]; then
            echo "ERROR: metadata lists \${n_metadata_ids} genome ID(s) but none matched a FASTA header" >&2
            exit 1
        fi
        if [[ \${n_refseq_ids} -gt 0 && \$(cat n_refseq.txt) -eq 0 ]]; then
            echo "WARNING: metadata lists \${n_refseq_ids} RefSeq genome ID(s) but none are present in the filtered FASTA; dedup will fall back to input order" >&2
        fi

        # Dedup by ID first, then by sequence. By ID (not `--by-name`) because
        # samtools keys on the reference name, so two records sharing an ID but
        # differing in description would still break RUN (see #758). By sequence
        # second, over an already ID-unique set, dropping records that duplicate
        # a retained sequence on either strand (aligners are strand-agnostic, so
        # a reverse-complement copy is redundant too).
        cat refseq.fna genbank.fna \\
            | seqkit rmdup --threads ${task.cpus} -D duplicate-ids.tsv \\
            | seqkit rmdup --by-seq --threads ${task.cpus} \\
                -D duplicate-seqs.tsv -o ${name_pattern}.fasta.gz
        rm -f refseq.fna genbank.fna

        # Log both duplicate sets. Sequence duplicates run to thousands of
        # groups on a production build, so report totals plus a sample rather
        # than the whole file. Column 1 of a seqkit -D file is the group size.
        for dup_file in duplicate-ids.tsv duplicate-seqs.tsv; do
            if [[ -s \${dup_file} ]]; then
                n_groups=\$(wc -l < \${dup_file})
                n_removed=\$(awk -F'\\t' '{ n += \$1 - 1 } END { print n }' \${dup_file})
                echo "\${dup_file}: \${n_groups} group(s), \${n_removed} record(s) removed. First 10 groups:"
                head -n 10 \${dup_file}
            else
                echo "\${dup_file}: no duplicates found."
            fi
        done
        echo "Output file contains" \$(zcat ${name_pattern}.fasta.gz | grep -c '^>') "sequences."
        """
}
