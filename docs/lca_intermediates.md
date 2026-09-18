# Intermediate TSV's relevant to `virus_hits_final.tsv.gz`

As a part of the `RUN` workflow, we emit two intermediate files that provide more context surrounding the results found in `virus_hits_final.tsv.gz`. More information about how these files are produced can be found in `docs/run.md`.

For most users, looking at these files will probably not be necessary. Some reasons you may want to look at these files include:
- You'd like to get more statistics on the LCA results
- You want to see whether a read had supplementary alignments
- You want to know the specific alignments that led to an LCA assignment

## Columns of `lca_final.tsv.gz`

 The `lca_final.tsv.gz` file contains LCA results organized into three data categories per sequence (`natural`, `combined`, and `artificial`). For details on how the LCA algorithm works and the definitions of these categories, see `docs/run.md`. Importantly, there is only one row per `seq_id` in this final table.

There are eight core statistical columns, which are repeated for each of the three data categories. The suffix of each column name (e.g. no suffix for `natural`, `_combined`, `_artificial`) indicates which category it describes. Additionally, there is one column, `seq_id`, which specifies the name of the read.

#### Core Column Definitions:

* `_taxid_lca`: The NCBI taxon ID for the lowest common ancestor (LCA) of all alignments in the group.
* `_n_assignments`: The total number of alignments in the group for that read.
* `_n_assignments_classified`: The subset of `_n_assignments` that are to classified taxa.
* `_taxid_top`: The taxon ID of the top-scoring alignment in the group.
* `_taxid_top_classified`: A boolean (`True`/`False`) value indicating if the top-scoring taxon is classified.
* `_length_normalized_score_min`: The minimum alignment score in the group.
* `_length_normalized_score_max`: The maximum alignment score in the group.
* `_length_normalized_score_mean`: The average alignment score for the group.

#### Column Groups:

* **natural** (e.g., `aligner_taxid_lca`, `aligner_n_assignments`): These columns provide statistics for *only* the natural alignments.
* **combined** (e.g., `aligner_taxid_lca_combined`, `aligner_n_assignments_combined`): These columns provide the same statistics but for *all* alignments (natural + artificial).
* **artificial** (e.g., `aligner_taxid_lca_artificial`, `aligner_n_assignments_artificial`): These columns provide the same statistics but *only* for the artificial alignments.

## Columns of `aligner_final.tsv.gz`

 The `aligner_final.tsv.gz` file contains the processed results for the aligner being used (bowtie2 or minimap2). Importantly, there may be multiple rows with the same `seq_id`, which indicates the existence of secondary or supplementary alignments.

*NB: For long read results, some of these columns are NA and some columns may not exist. These columns have been noted below.*

The `_rev` suffix names the mate, not the strand: unsuffixed columns describe mate 1 and `_rev` columns mate 2, and either mate can align to either strand.

- `seq_id`: Name of read
- `genome_id`: GenBank ID for best viral genome match to read, as identified by our aligner (bowtie2 or minimap2). "Best" means having the highest length normalized alignment score. For paired-end data, this is the highest-scoring match across forward and reverse reads. 
- `genome_id_all`: GenBank IDs for viral genomes matching forward and reverse reads, joined by "/". For single-read data, this is the same as `genome_id`
- `taxid`: NCBI taxon ID for taxon best matching read, as identified by our aligner (bowtie2 or minimap2). For paired-end data, this is the highest-scoring match across forward and reverse reads. 
- `taxid_all`: NCBI taxon ID for taxons matching forward and reverse reads, joined by "/". For single-read data, this is the same as `taxid`.
- `fragment_length`: Inferred fragment length, as calculated by the aligner: Bowtie2's TLEN, which counts soft-clipped bases (NA if the two mates align to different genome IDs). For single-read data, this column doesn't exist.
- `best_alignment_score`: Alignment score (directly from aligner) of best-scoring alignment (for paired-end data, score for forward read's best alignment). 
- `best_alignment_score_rev`: Alignment score of best-scoring alignment of reverse read. For single-read data, this column doesn't exist.
- `next_alignment_score`: Alignment score of second-best alignment (for paired-end data, score for forward read's second-best alignment; NA for minimap2)
- `next_alignment_score_rev`: Score of second-best alignment of reverse read. For single-read data, this column doesn't exist.
- `genome_id_fwd`: GenBank ID for viral genome matching forward read. For single-read data, this column doesn't exist.
- `genome_id_rev`: GenBank ID for viral genome matching reverse read. For single-read data, this column doesn't exist.
- `taxid_fwd`: NCBI taxon ID for taxon matching forward read. For single-read data, this column doesn't exist.
- `taxid_rev`: NCBI taxon ID for taxon matching reverse read. For single-read data, this column doesn't exist.
- `edit_distance`: Edit distance between read and aligned genome (for paired-end data, edit distance for forward read)
- `edit_distance_rev`: Edit distance between reverse read and aligned genome. For single-read data, this column doesn't exist.
- `ref_start`: Location of start of alignment on reference, excluding soft-clipped bases (for paired-end data, mate 1's alignment)
- `ref_start_rev`: Location of start of alignment of mate 2 on reference, excluding soft-clipped bases. For single-read data, this column doesn't exist.
- `ref_start_unclipped`: Location of the first reference base the read covers, with clipped bases counted as if they had aligned. `ref_start` minus the alignment's leading clip (for paired-end data, mate 1's). NA where the aligner reported no CIGAR.
- `ref_end_unclipped`: Location of the last reference base the read covers, with the same convention (for paired-end data, mate 1's).
- `ref_start_unclipped_rev`: Location of the first reference base mate 2 covers, with clipped bases. For single-read data, this column doesn't exist.
- `ref_end_unclipped_rev`: Location of the last reference base mate 2 covers, with clipped bases. For single-read data, this column doesn't exist.
- `map_qual`: Mapping quality (MAPQ) as returned by bowtie2/minimap2 (for paired-end data, mapping quality of forward read)
- `map_qual_rev`: Mapping quality (MAPQ) of reverse read. For single-read data, this column doesn't exist.
- `cigar`: CIGAR string representing alignment of read to aligned genome (for paired-end data, CIGAR string for forward read). Note that this is the CIGAR string as returned by the aligner. If `query_rc` is true, you should reverse-complement the query sequence before comparing it against the CIGAR string. 
- `cigar_rev`: CIGAR string representing alignment of reverse read to aligned genome. For single-read data, this column doesn't exist.
- `query_len`: Length of read, after trimming (for paired-end data, length of forward read)
- `query_len_rev`: Length of reverse read. For single-read data, this column doesn't exist.
- `query_seq`: Sequence of read (for paired-end data, sequence of forward read). Not reverse-complemented (we undo any reverse-complement performed by aligner). Note that forward and reverse read is arbitrary (for those looking for duplicates, this means that you might try looking for duplicates by reversing the reads).
- `query_seq_rev`: Sequence of reverse read. Like query_seq, not reverse-complemented. For single-read data, this column doesn't exist.
- `query_rc`: A boolean (`True`/`False`) value indicating if the query was reverse-complemented by aligner.
- `query_rc_by_rev`: A boolean (`True`/`False`) value indicating if the (reverse-read) query was reverse-complemented by aligner. For single-read data, this column doesn't exist.
- `query_qual`: PHRED quality scores for read (for paired-end data, quality scores for forward read). Like `query_seq`, not reverse-complemented. 
- `query_qual_rev`: PHRED quality scores for reverse read. For single-read data, this column doesn't exist.
- `length_normalized_score_fwd`: Length-normalized alignment score for forward read. For single-read data, this column doesn't exist.
- `length_normalized_score_rev`: Length-normalized alignment score for reverse read. For single-read data, this column doesn't exist.
- `length_normalized_score`: Length-normalized alignment score: `best_alignment_score` divided by natural log of `query_len`. (For paired-end data, max of scores for forward and reverse reads.)
- `pair_status`: Pair status (UU for unpaired, CP for concordant pair, DP for discordant pair, UP for unaligned pair). For single-read data, this column doesn't exist.
- `classification`: Alignment type (primary, secondary or supplementary)

