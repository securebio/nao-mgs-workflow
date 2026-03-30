# Ultima DOWNSTREAM: Blast-Every-Read Validation + Clade Counting

## Strategy

Run Ultima viral hits (from RUN, single-end format) through DOWNSTREAM with two
adaptations:

1. **Blast every read** instead of clustering first — use the existing pipeline's
   own escape hatch (`validation_cluster_identity=1`, `validation_n_clusters=1000000`)
   rather than writing new code.
2. **Clade counting without duplicate marking** — add a `prim_align_dup_exemplar`
   column equal to `seq_id` so `COUNT_READS_PER_CLADE` runs as-is, then drop the
   meaningless dedup columns in postprocessing.

No new subworkflows. No new modules. Just a config file and a small patch to the
platform branching in `downstream.nf`.

---

## BLAST Settings for Ultima

The three LLM assessments in `blast_settings_notes.txt` converge on one point: Ultima's
dominant error mode is homopolymer indels, which inflate gap counts and depress percent
identity relative to Illumina. The current pipeline has two BLAST configs:

| Parameter | Illumina | ONT |
|-----------|:--------:|:---:|
| `blast_perc_id` | 60 | 0 |
| `blast_qcov_hsp_perc` | 30 | 0 |
| `blast_max_rank` | 10 | 5 |
| `blast_min_frac` | 0.9 | 0.9 |

ONT uses `perc_id=0` and `qcov=0` because long reads with high indel rates would be
heavily penalized by identity thresholds. Ultima is much closer to Illumina — short
reads (~200-300bp), low overall error rate (~0.5-1%), with errors concentrated in
homopolymers — but not identical, because the errors are indels rather than
substitutions.

### Recommended Ultima BLAST settings

```
blast_perc_id         = 40
blast_qcov_hsp_perc   = 20
blast_max_rank        = 10
blast_min_frac        = 0.9
```

**Rationale:**

- **`blast_perc_id = 40` (vs Illumina 60):** This is the minimum percent identity for
  BLAST to *return* an alignment (pre-filter, not final threshold). Homopolymer indels
  reduce `pident` because BLAST counts each gap position against alignment length. A
  250bp Ultima read with 5 homopolymer indels could lose ~2-4% identity from gaps alone,
  on top of any real divergence from the reference. Lowering from 60 to 40 ensures we
  don't drop real viral hits at the BLAST return stage. The bitscore-based filtering
  (`blast_min_frac`, `blast_max_rank`) and the downstream LCA + taxonomic distance
  validation are the real quality gates — `perc_id` just controls what BLAST bothers to
  report. Being permissive here costs some extra BLAST output rows but doesn't affect
  final call quality.

- **`blast_qcov_hsp_perc = 20` (vs Illumina 30):** Query coverage measures what fraction
  of the read aligns. Homopolymer indels can cause BLAST to split alignments or truncate
  them at problematic regions. Lowering from 30 to 20 gives headroom. For a 250bp read,
  20% = 50bp minimum aligned — still meaningful.

- **`blast_max_rank = 10` (same as Illumina):** Keep the same number of top hits. Since
  we're BLASTing every read (not just cluster reps), we want enough hits for LCA to work
  well but don't need more than Illumina uses.

- **`blast_min_frac = 0.9` (same as both):** Bitscore-based filtering is the main
  quality gate and is error-profile-agnostic — a bitscore already accounts for gaps.
  No change needed.

### Why not just copy ONT settings (perc_id=0, qcov=0)?

ONT's settings are maximally permissive because ONT reads are long (thousands of bp)
with high error rates (~5-15%). This produces huge BLAST output that's expensive to
process. Ultima reads are short and relatively accurate — we can afford real thresholds.
Using 0/0 would work scientifically but would produce unnecessarily large intermediate
files and slow down the sort/filter/LCA steps.

### Why not keep Illumina settings (perc_id=60, qcov=30)?

Probably fine for most reads, but risks silently dropping viral hits in homopolymer-rich
regions. Since this is an R&D pilot where we want to *characterize* Ultima's behavior,
being moderately permissive is better than discovering later that we filtered out
interesting edge cases.

---

## Implementation

### 1. Create `configs/downstream_ultima.config`

Copy `configs/downstream_ont.config` and modify:

```groovy
params {
    mode = "downstream"
    platform = "ultima"

    // Directories
    base_dir = <PATH_TO_DIRECTORY>
    ref_dir = <PATH TO REFERENCE DIRECTORY>

    // Files
    input_file = "${launchDir}/input.csv"

    // Blast-every-read: same trick as ONT config
    validation_cluster_identity = 1
    validation_n_clusters = 1000000

    // BLAST settings: between Illumina and ONT (see rationale above)
    blast_db_prefix = "core_nt"
    blast_perc_id = 40
    blast_qcov_hsp_perc = 20
    blast_min_frac = 0.9
    blast_max_rank = 10
    taxid_artificial = 81077

    // No duplicate marking for Ultima
    // (handled by the downstream.nf patch below)

    // AWS Batch job queue
    queue = "BATCH_QUEUE_NAME"
}

includeConfig "${projectDir}/configs/logging_downstream.config"
includeConfig "${projectDir}/configs/containers.config"
includeConfig "${projectDir}/configs/resources.config"
includeConfig "${projectDir}/configs/profiles.config"
includeConfig "${projectDir}/configs/output.config"
process.queue = params.queue
```

### 2. Patch `workflows/downstream.nf` — add Ultima platform branch

The existing code has `if (params.platform == "ont") { ... } else { ... }`. Add an
Ultima branch that skips duplicate marking but still runs clade counting:

```groovy
if (params.platform == "ont") {
    // ... existing ONT code (unchanged) ...
}
else if (params.platform == "ultima") {
    // Ultima: skip duplicate marking, but add prim_align_dup_exemplar = seq_id
    // so clade counting works (treats every read as non-duplicate)
    viral_hits_ch = SORT_ONT_HITS(concat_ch.hits, "seq_id").sorted
    // Pad PE columns with NA (same as ONT) + set dup_exemplar = seq_id
    def pad_cols = [
        "query_len_rev", "query_seq_rev", "query_qual_rev",
        "prim_align_fragment_length",
        "prim_align_best_alignment_score_rev",
        "prim_align_edit_distance_rev",
        "prim_align_ref_start_rev", "prim_align_query_rc_rev",
        "prim_align_pair_status"
    ].join(",")
    viral_hits_ch = PAD_ONT_COLUMNS(viral_hits_ch, pad_cols, "NA", "padded").output
    // Add prim_align_dup_exemplar as a copy of seq_id (no read is a "duplicate")
    viral_hits_ch = ADD_DUP_EXEMPLAR(viral_hits_ch)
    dup_output_ch = Channel.empty()
    // Run clade counting (works because prim_align_dup_exemplar == seq_id for all reads)
    clade_counts_ch = COUNT_READS_PER_CLADE(viral_hits_ch, viral_db).output
}
else {
    // ... existing Illumina/short-read code (unchanged) ...
}
```

This requires one small new process, `ADD_DUP_EXEMPLAR`, that copies the `seq_id`
column into a new `prim_align_dup_exemplar` column. This is a one-liner — for example
using awk or the existing `addFixedColumn` module pattern. Alternatively, you can
probably use a second `ADD_FIXED_COLUMN` call, but that adds a literal string, not a
copy of another column. The simplest approach:

```groovy
process ADD_DUP_EXEMPLAR {
    label "single"
    label "python"
    input:
        tuple val(sample), path(tsv)
    output:
        tuple val(sample), path("${sample}_with_dup.tsv.gz"), emit: output
    script:
    """
    python3 -c "
import gzip, csv, sys
with gzip.open('${tsv}', 'rt') as inf, gzip.open('${sample}_with_dup.tsv.gz', 'wt') as outf:
    reader = csv.DictReader(inf, delimiter='\t')
    fields = reader.fieldnames + ['prim_align_dup_exemplar']
    writer = csv.DictWriter(outf, fieldnames=fields, delimiter='\t')
    writer.writeheader()
    for row in reader:
        row['prim_align_dup_exemplar'] = row['seq_id']
        writer.writerow(row)
"
    """
}
```

Or even simpler — just use awk inline without a separate process. Either works for R&D.

### 3. Handle `single_end` in VALIDATE_VIRAL_ASSIGNMENTS

Line 51 of `validateViralAssignments/main.nf` currently passes:

```groovy
Channel.of(params.platform == "ont")
```

This needs to also be true for Ultima (single-end reads). Change to:

```groovy
Channel.of(params.platform == "ont" || params.platform == "ultima")
```

This makes the merge-join step pass Ultima reads through unchanged (like ONT) rather
than trying to run BBMERGE on them (which would fail on non-interleaved data).

### 4. Postprocessing: drop dedup columns from clade counts

After the pipeline finishes, strip the meaningless dedup columns:

```bash
for f in *_clade_counts.tsv.gz; do
    zcat "$f" | cut -f1,2,3,4,6 | gzip > "${f%.tsv.gz}_clean.tsv.gz"
done
```

This keeps `group, taxid, parent_taxid, reads_direct_total, reads_clade_total` and
drops `reads_direct_dedup, reads_clade_dedup` (which are identical to the total columns
since no reads are marked as duplicates).

---

## Summary of Changes

| File | Change | Lines |
|------|--------|:-----:|
| `configs/downstream_ultima.config` | New config file (copy of ONT config with tuned BLAST params) | ~30 |
| `workflows/downstream.nf` | Add `else if (params.platform == "ultima")` branch | ~15 |
| `workflows/downstream.nf` (or new module) | `ADD_DUP_EXEMPLAR` process/inline | ~10 |
| `subworkflows/local/validateViralAssignments/main.nf` | `platform == "ont"` → `platform in ["ont", "ultima"]` | 1 |
| **Total new/changed code** | | **~55** |

Plus a postprocessing one-liner to drop dedup columns.

---

## Compute Cost Considerations

Blasting every read instead of cluster representatives is significantly more expensive.
For context:

- The Illumina config clusters at 95% identity and validates 20 reps per species — this
  reduces the number of BLAST queries by ~100-1000x.
- "Blast every read" means every single viral hit gets BLASTed against core_nt.

For wastewater metagenomics, the number of viral hits is typically a small fraction of
total reads (post-BBDuk + alignment), so this is feasible. But if a sample has, say,
50K viral hits instead of the ~500 cluster reps that would normally be BLASTed, expect
BLAST runtime to scale roughly proportionally.

This is acceptable for a 30-sample pilot, especially on AWS Batch where parallelism is
cheap. If it turns out to be too slow, you can always fall back to clustering
(`validation_cluster_identity=0.95`, `validation_n_clusters=20`) — it's just a config
change.

---

## What the Results Tell You

### Directly comparable to Illumina DOWNSTREAM output
- Clade counts (total only, not dedup)
- Validation status of viral hits (same BLAST → LCA → taxonomic distance pipeline)
- Annotated viral hits with validation columns

### Different from Illumina
- No duplicate marking / dedup counts
- More permissive BLAST thresholds (40/20 vs 60/30) — means more BLAST hits are
  considered during LCA, which could slightly change validation outcomes for borderline
  cases. This is intentional and conservative.
- Every read is independently validated (no cluster-based propagation) — this is
  strictly more informative than the Illumina approach, just more expensive.
