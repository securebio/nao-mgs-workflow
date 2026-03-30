# Ultima Adaptation: Middle Path

## Strategy
Create a new `EXTRACT_VIRAL_READS_ULTIMA` subworkflow that cherry-picks the best
components from both paths, optimized for Ultima's error profile while maximizing
reuse of existing modules. The guiding principle: use the tool that's best-suited
for each specific task, rather than forcing everything through one pipeline.

## Design Philosophy
- **Alignment**: Use minimap2 `-ax sr` for viral identification (indel-tolerant,
  fast, well-suited to Ultima). Use Bowtie2 for human/contaminant depletion
  (well-benchmarked for depletion, conservative).
- **Preprocessing**: Use FASTP (from Illumina path) -- purpose-built for short
  variable-length reads, good quality/complexity filtering.
- **Kmer screening**: Keep BBDuk but with relaxed parameters for indel tolerance.
- **Profiling**: Use BBDuk for ribo separation, Kraken2/Bracken for taxonomy
  (from Illumina path) -- avoids needing sr-preset minimap2 ribo indexes.
- **No new bioinformatics tools** -- everything uses existing modules.

---

## Input Handling

Same as both other paths:
1. Concatenate per-sample FASTQs before pipeline entry.
2. Single-end samplesheet (`sample,fastq`).
3. Add `"ultima"` platform to loadSampleSheet.

## Viral Read Extraction: EXTRACT_VIRAL_READS_ULTIMA

### Depletion order: why BBDuk -> deplete -> viral alignment

The three possible orderings and their tradeoffs:

**A. Illumina-style (viral first):** BBDuk -> align to viruses -> deplete human ->
deplete contaminants -> retroactively filter viral SAM

This is what the Illumina path does. BBDuk selects ~1% of reads with viral kmer
matches. ALL of those get aligned to the virus database, producing a viral SAM.
Then the same reads go through human and contaminant depletion. Finally,
FILTER_VIRAL_SAM cross-references the viral SAM with the list of surviving reads
to retroactively remove any viral alignments for reads that turned out to be
human/contaminant.

The advantage is that viral alignment scores are available for every kmer-matching
read, even those later removed as contaminants. The disadvantage is requiring
FILTER_VIRAL_SAM for the retroactive cleanup -- the module with deep paired-end
assumptions that's the highest-risk item in the Illumina path.

**B. ONT-style (deplete first, no pre-filter):** deplete human (ALL reads) ->
deplete contaminants -> align to viruses

This is what the ONT path does. ALL reads go through human depletion (333M reads),
then contaminant depletion (~300M remaining), then viral alignment (~280M remaining).
The advantage is simplicity -- no retroactive filtering needed. The disadvantage is
massive compute cost: ~900M total alignments across the three steps. For 10 billion
reads across 30 samples, this would be prohibitively expensive.

**C. Middle path (BBDuk pre-filter + deplete first):** BBDuk -> deplete human ->
deplete contaminants -> align to viruses

This combines BBDuk's fast kmer pre-filtering (~1% of reads survive) with the
deplete-first ordering. After BBDuk, only ~3M reads/sample enter the pipeline.
Human and contaminant depletion process this small set quickly, and viral alignment
sees only clean reads. No FILTER_VIRAL_SAM needed.

**Why C is the right choice here:**
- **Fast**: BBDuk processes 333M reads with fast kmer matching (not alignment).
  Everything downstream sees only ~3M reads.
- **No retroactive filtering**: Reads reaching minimap2 are already clean, so
  PROCESS_VIRAL_MINIMAP2_SAM output doesn't need post-hoc contaminant removal.
  This avoids FILTER_VIRAL_SAM entirely.
- **Equivalent results**: A read that matches viral kmers but maps to human gets
  removed at the Bowtie2-human step, never reaching viral alignment. In the
  Illumina path, the same read gets a viral alignment and then gets retroactively
  filtered out. The final viral hits are the same in both cases.
- **IRB-compatible**: Human reads are removed before viral alignment results are
  generated, which was the motivation for the deplete-first ordering in the ONT path.

**When the order matters -- chimeric reads**: If a read maps to both human and a
virus (chimeric reads, integrated viral sequences), the deplete-first approach
removes it as human without ever checking its viral alignment. The Illumina
approach would produce both alignments and let FILTER_VIRAL_SAM adjudicate.
This is relevant for our analysis -- we do care about chimeric reads. For the
pilot, the deplete-first ordering is acceptable (and matches the IRB-compliant
ONT path), but see the note below on what switching to viral-first would require.

**Future option: switching to viral-first ordering for chimeric read recovery**

If chimeric reads prove important, the ordering could be changed to:
BBDuk → minimap2-virus → Bowtie2-human → Bowtie2-other → retroactive SAM filtering.

This would require:
1. **A single-end FILTER_VIRAL_SAM**: The existing `filter_viral_sam.py` has deep
   paired-end assumptions (synthetic mate creation, pair-based grouping, YS:i tag
   assertions). A `--single-end` mode would need to be added that skips all mate
   logic and simplifies to: (a) keep only reads present in the clean FASTQ,
   (b) apply score threshold, (c) sort by read ID. The pair-grouping and
   mate-synthesis code is irrelevant for single-end and can be bypassed.
2. **Cross-aligner SAM/FASTQ coordination**: The viral SAM would come from minimap2,
   but the clean FASTQ (surviving depletion) would come from Bowtie2 output. The
   FILTER_VIRAL_SAM step would need to cross-reference minimap2 read IDs against
   Bowtie2's surviving read list. The existing logic does this for Bowtie2-to-Bowtie2,
   so the read ID matching should still work (read IDs don't change between aligners).
3. **Score threshold recalibration**: minimap2 alignment scores are on a different
   scale than Bowtie2. The `bt2_score_threshold` parameter would need a minimap2
   equivalent.

This is moderate additional work (mainly item 1), and could be done as a follow-up
if the pilot reveals chimeric reads are a significant concern.

### Understanding the Illumina path's BBDuk flow (context for design)

The existing Illumina path uses BBDuk as a **positive enrichment filter**: it
identifies reads with viral kmer hits (`BBDUK_HITS_INTERLEAVE`), and ONLY those
reads proceed through the alignment chain. Reads without viral kmer matches are
excluded from viral analysis entirely. Specifically:
- `bbduk_ch.fail` = reads caught by the filter (matched viral kmers) -> FASTP -> Bowtie2
- `bbduk_ch.reads` = reads that passed through (no viral kmers) -> discarded from viral path

This is an enrichment strategy: with ~333M reads/sample and <1% viral, BBDuk
reduces the alignment input by ~99%, saving enormous compute. But it means BBDuk
is a **hard filter** -- any viral read that fails kmer matching is permanently lost.

For Ultima, this is a meaningful concern: homopolymer indels break exact kmer
matches. The mitigation is to relax BBDuk parameters (lower k, allow hamming
distance). But it's worth flagging as a known sensitivity trade-off.

Note: `BBDUK_HITS_INTERLEAVE` is paired-end-specific (takes two FASTQ files and
interleaves). For single-end Ultima, we'll use the regular `BBDUK` module with
`interleaved=false` and comparable parameters (`minkmerhits` instead of
`minkmerfraction`). This requires a minor adaptation since BBDUK uses
`minkmerfraction` not `minkmerhits` -- we'd either add `minkmerhits` support to
BBDUK or create a small single-end variant of BBDUK_HITS.

### Step 1: FASTP quality/complexity cleanup
**Module**: FASTP (existing, from Illumina path)
**Mode**: Single-end (not interleaved)
**Parameters**:
- `--length_required 50` (min length post-trim)
- `--cut_front --cut_tail --cut_mean_quality 15` (light quality trim -- Ultima
  Q-scores are neural-network-derived, so be more conservative than Illumina's Q20)
- `--trim_poly_x` (poly-A/T artifact removal -- important for RNA-seq + Ultima)
- `--low_complexity_filter` (entropy filter for homopolymer junk)
- `--dont_eval_duplication` (irrelevant for metagenomics)
- No `--correction` or `--detect_adapter_for_pe` (single-end)
- Keep adapter trimming enabled (`--adapter_sequence` / `--adapter_fasta` with standard
  Illumina adapters). The Minnesota RNA-seq benchmark found residual Illumina adapter
  sequences (~70bp) surviving on-instrument Trimmer processing when Illumina-adapter
  library prep kits were used on Ultima -- exactly our scenario. If Trimmer already
  removed everything, FASTP finds nothing. If residuals remain, they get caught.

**Rationale**: FASTP over FILTLONG because it's designed for short reads, has
built-in poly-X trimming and low-complexity filtering, and produces QC JSON output
that integrates with the existing RUN_QC reporting.

### Step 2: BBDuk positive viral kmer screen
**Module**: BBDUK or BBDUK variant (see note above)
**Parameters**: k=21, minkmerhits=1, hdist=1
**Change from Illumina default**: Lower k (24->21) and add hdist=1 to tolerate
single-base indels that break kmer matches in homopolymer regions.
**Output**: Only viral-kmer-positive reads proceed. This keeps the compute cost
of downstream alignment manageable for 10B total reads.
**Risk**: Some viral reads with homopolymer indels will be missed. Relaxed params
mitigate but don't eliminate this.
**Alternative**: Skip BBDuk and send all FASTP-cleaned reads through the full
depletion + alignment chain. Much more expensive but no sensitivity loss.

### Step 3: Bowtie2 human depletion
**Module**: BOWTIE2 (existing, from Illumina path)
**Index**: Existing `bt2-human-index` (no new indexes needed)
**Mode**: Single-end (not interleaved, unmapped_flag=4)
**Parameters**: `--local --very-sensitive-local --rdg 3,1 --rfg 3,1`
**Rationale**: Bowtie2 is well-benchmarked specifically for human depletion (the
GPT notes cite a recent benchmark finding Bowtie2 `--very-sensitive-local` as
best for host depletion). Relaxed gap penalties help catch human reads with
homopolymer indels, improving depletion completeness. Using existing Bowtie2
indexes avoids building new minimap2 human indexes.

**Note on compute**: After BBDuk pre-filtering, only ~3M reads/sample reach this
step. Bowtie2 depletion of this small set takes minutes, not hours. Most of these
reads are genuinely viral (that's what BBDuk selected for), so few will map to human.

### Step 4: Bowtie2 contaminant depletion
**Module**: BOWTIE2 (existing)
**Index**: Existing `bt2-other-index`
Same parameters and rationale as human depletion.

### Step 5: Minimap2 viral alignment
**Module**: MINIMAP2 (existing, from ONT path -- use the streamed version, not
MINIMAP2_NON_STREAMED)
**Index**: New `mm2-sr-virus-index` (built with `-x sr` preset)
**Alignment params**: `-ax sr -N 10 -t ${task.cpus}` (short-read mode, 10
alignments for LCA, use all allocated CPUs)
**Rationale**: This is the core advantage of the middle path. Minimap2 `-ax sr`
handles Ultima's indel errors naturally without gap penalty tuning, and is ~3x
faster than Bowtie2 for reads >100bp. The `-N 10` flag provides multiple
alignments for LCA-based taxonomic assignment.

**Threading note**: The existing MINIMAP2 processes don't pass `-t` to minimap2
(it defaults to 3 threads while 16-32 CPUs are allocated). Adding `-t ${task.cpus}`
is a one-line fix that dramatically improves throughput. This should be done for the
ONT path too.

**Why streamed MINIMAP2 (not MINIMAP2_NON_STREAMED)**: The codebase has two minimap2
process variants. MINIMAP2_NON_STREAMED exists because the contaminant ("other")
index is a large composite multi-reference database that requires minimap2's
`--split-prefix` flag to avoid memory overflow during alignment. `--split-prefix`
writes intermediate results to disk, which is incompatible with piping SAM through
`tee` to samtools -- hence the non-streamed version writes a complete SAM to disk
first, then partitions it. The virus and human indexes are smaller/simpler and work
fine with the streamed version (which pipes SAM directly through samtools, no
intermediate file). In the ONT path, MINIMAP2_VIRUS and MINIMAP2_HUMAN use streamed,
while MINIMAP2_CONTAM uses non-streamed. Since the middle path uses minimap2 only
for virus alignment, the streamed version is correct.

**Why minimap2 for viral but Bowtie2 for depletion**: Different optimization
targets. For viral identification, we need to sensitively detect divergent
sequences across a large reference database -- minimap2's indel tolerance and
speed excel here. For depletion, we need reliable removal against a known genome
-- Bowtie2 is well-validated for this specific task, and reusing existing indexes
avoids building 2 additional minimap2 indexes.

### Step 6: SAM processing
**Module**: PROCESS_VIRAL_MINIMAP2_SAM (existing, from ONT path)
**Input**: The SAM and reads_mapped FASTQ from MINIMAP2_VIRUS, joined by sample.
**Rationale**: Since viral alignment uses minimap2, use the minimap2-specific SAM
processing module. This already handles single-end data correctly and understands
minimap2's SAM tags (AS, NM, etc.). No paired-end column issues.

The ONT path has an extra step here -- EXTRACT_VIRAL_FILTERED_READS -- that pulls
original (unmasked) read sequences for virus-mapped reads. **We don't need this**
because the middle path has no masking step (FASTP cleans reads without masking).
The reads entering minimap2 already have their final sequences, so
`reads_mapped` from MINIMAP2 can be used directly as the FASTQ input to
PROCESS_VIRAL_MINIMAP2_SAM.

PROCESS_VIRAL_MINIMAP2_SAM internally sorts both SAM and FASTQ by read ID before
processing (via `sort_sam.py` and `sort_fastq.py`), so no pre-sorting step is
needed. The resource label is `single` (1 CPU, 4GB RAM), which is fine since this
only processes virus-mapped reads (<1% of total).

**Major advantage -- avoids FILTER_VIRAL_SAM entirely**: Because we do depletion
BEFORE viral alignment, reads reaching minimap2 are already human/contaminant-free.
No retroactive contaminant filtering needed. This sidesteps the deep paired-end
assumptions in `filter_viral_sam.py` entirely (see depletion order section above).

### Step 7: Sort processed TSV
**Module**: SORT_TSV (existing, used by ONT path as SORT_MINIMAP2_VIRAL)
**Sort key**: `seq_id`
**Rationale**: LCA_TSV requires input sorted by the group field. The ONT path
does this same sort; it was missing from the original version of this plan.

### Step 8: LCA taxonomic assignment
**Module**: LCA_TSV (existing, shared by both paths)
**Parameters**: Same as current pipeline (group_field=seq_id, taxid_field=taxid,
score_field=length_normalized_score, prefix=aligner)

### Step 9: Output processing
**Module**: PROCESS_LCA_ALIGNER_OUTPUT (existing, shared by both paths)
**Column lists**: Use ONT-style column definitions (without paired-end-specific
columns like `pair_status`, `fragment_length`, `*_rev` fields). Output schemas
can be ignored/disabled for this pilot.

### Complete flow diagram
```
Raw reads (single-end FASTQ, concatenated per sample)
  -> FASTP (QC, poly-X trim, complexity filter, adapter safety net)
  -> BBDuk (positive viral kmer screen, k=21, hdist=1)
     -> viral-kmer-positive reads only continue
  -> Bowtie2-human (depletion, discard mapped, SE mode, relaxed gap penalties)
  -> Bowtie2-other (depletion, discard mapped, SE mode, relaxed gap penalties)
  -> Minimap2-virus (alignment, -ax sr -N 10 -t cpus, mm2-sr-virus-index)
     -> SAM + reads_mapped FASTQ
  -> PROCESS_VIRAL_MINIMAP2_SAM (SAM + reads_mapped FASTQ -> TSV)
  -> SORT_TSV (sort by seq_id)
  -> LCA_TSV (taxonomic assignment)
  -> PROCESS_LCA_ALIGNER_OUTPUT (column selection, final output)
  => virus_hits.tsv.gz
```

---

## Subset/Trim (SUBSET_TRIM)
- Route through single-end subsetting (`SUBSET_READS_SINGLE_TARGET`)
- Use FASTP for trimming (single-end mode, same parameters as Step 1)
- No interleaving needed

## QC (RUN_QC)
- Works as-is with single-end data. FastQC handles single-end fine.

## Taxonomic Profiling (PROFILE)

### Ribo separation
- Use BBDuk (from Illumina path) in single-end mode (`interleaved=false`)
- Standard parameters (min_kmer_fraction=0.4, k=27) -- ribo sequences are
  well-conserved with few homopolymers, so kmer sensitivity loss is minimal
- Avoids building an mm2-sr-ribo-index

### Taxonomy (Kraken2/Bracken)
- Single-end: skip BBMERGE/JOIN_FASTQ (pass-through via MERGE_JOIN_READS)
- Kraken2 with standard k=35 -- Ultima reads are in the same length range as
  Illumina, and Kraken's minimizer approach is somewhat robust to occasional
  indels. Expect a modest sensitivity decrease vs Illumina.
- Bracken abundance estimation with standard parameters

---

## Infrastructure Changes

1. **loadSampleSheet**: Add `"ultima"` platform with `"single"` endedness.
2. **run.nf**: Add third branch for `params.platform == "ultima"` that calls
   `EXTRACT_VIRAL_READS_ULTIMA`.
3. **New subworkflow**: `subworkflows/local/extractViralReadsUltima/main.nf` --
   assembles existing modules in the order described above. (All three plans now
   converge on needing a new subworkflow -- the single-end changes are too pervasive
   to cleanly piggyback on either EXTRACT_VIRAL_READS_SHORT or _ONT.)
4. **BBDuk**: Either add `minkmerhits` support to the single-end BBDUK process,
   or create a small `BBDUK_HITS_SINGLE` variant.
5. **MINIMAP2_INDEX**: Parameterize the preset (currently hardcoded `lr:hq`).
   Need `-x sr` for the virus index.
6. **INDEX workflow**: Build 1 new minimap2 index: `mm2-sr-virus-index`.
   (Human/other depletion uses existing Bowtie2 indexes.)
   Can be done manually on EC2 -- just `minimap2 -x sr -d mm2_index.mmi reference.fasta`.
7. **Minimap2 threading**: Add `-t ${task.cpus}` to MINIMAP2 and MINIMAP2_NON_STREAMED.
   Benefits ONT runs too -- should be done regardless.
8. **No new containers or bioinformatics tools needed.**

---

## Pros
- **Best viral sensitivity**: minimap2 for indel-tolerant viral identification.
- **Well-validated depletion**: Bowtie2 for human/contaminant removal.
- **Only 1 new index** (mm2-sr-virus-index) vs 4 in the full ONT path.
  Human/other depletion reuses existing Bowtie2 indexes.
- **Clean single-end handling**: PROCESS_VIRAL_MINIMAP2_SAM already handles
  single-end correctly. No paired-end column workarounds. No need to rework
  FILTER_VIRAL_SAM (the highest-risk item in the Illumina path).
- **Avoids FILTER_VIRAL_SAM entirely**: The deplete-first flow means viral SAM
  output doesn't need retroactive contaminant filtering, sidestepping the deep
  paired-end assumptions in `filter_viral_sam.py` (synthetic mate creation,
  pair-based grouping, YS:i tag assertions) that would need substantial rework
  for single-end data.
- **Good preprocessing**: FASTP provides poly-X trimming, complexity filtering,
  and QC JSON in one step.
- **Partial comparability**: Bowtie2 depletion keeps that step comparable to
  Illumina. Minimap2 for viral alignment may capture reads Bowtie2 misses,
  which is arguably what you want for evaluating Ultima's viral detection
  capability.
- **Fast**: BBDuk reduces input by ~99% before alignment. Minimap2 is ~3x faster
  than Bowtie2 at the viral alignment step. With threading fix, all alignment
  steps use full allocated CPUs.
- **No masking step needed**: FASTP handles quality/complexity cleanup. No
  MASK_FASTQ_READS and no EXTRACT_VIRAL_FILTERED_READS to undo masking.

## Cons
- **New subworkflow needed**: More code than the other two approaches (though
  it's mostly wiring existing modules together, ~100 lines of Nextflow).
- **1 new minimap2 index**: Must build mm2-sr-virus-index (one-time cost,
  straightforward -- single minimap2 command on EC2).
- **BBDuk sensitivity loss**: Same concern as Illumina path -- the kmer
  enrichment filter will miss some viral reads with homopolymer indels.
- **Score threshold calibration**: Need to determine appropriate minimap2
  alignment score cutoff. Can adapt from the ONT path's approach.
- **Mixed aligner pipeline**: Using two different aligners is conceptually
  more complex, though mechanically straightforward.

## Estimated Effort
Medium. New subworkflow (~100 lines, composed of existing modules), one new
minimap2 index, BBDuk single-end adaptation, and parameter tuning. No new
tools, containers, or fundamental pipeline logic. Notably, this path avoids
the FILTER_VIRAL_SAM rework that is the highest-risk item in the Illumina path,
so while more code is written (new subworkflow), the code risk is lower --
it's mostly wiring together modules that already work for single-end data.

---

## Recommendation

This is my recommended approach for the Ultima pilot. It provides the best
viral sensitivity (minimap2), well-validated depletion (Bowtie2), and good
preprocessing (FASTP), while requiring fewer new indexes than the full ONT
path. The new subworkflow is straightforward to build from existing modules.

The deeper code audit reinforces this recommendation: the Illumina path's
FILTER_VIRAL_SAM is the single hardest piece to adapt for single-end data,
and this path avoids it entirely by using the deplete-first flow with
PROCESS_VIRAL_MINIMAP2_SAM (already single-end compatible). The Illumina
path's effort estimate has been revised upward from small-medium to medium
specifically because of this issue.

For maximum Illumina comparability, consider running both this path AND the
Illumina path on a subset of samples, to quantify how much the aligner choice
affects viral hit concordance.
