# Ultima Adaptation: New Tools Considered (and Rejected)

## Context

The middle path plan (`plan_middle_path.md`) has a "no new bioinformatics tools"
constraint. This document evaluates what we'd gain by relaxing that constraint.

**Conclusion: the benefit is minimal.** The middle path as designed is ~90-95% of
optimal for this pilot. The remaining gaps are small, and for the pilot's goal
(evaluate Ultima vs Illumina), adding new tools would confound the comparison by
introducing analysis-method differences alongside platform differences.

---

## Tools Evaluated, Ranked by Leverage

### 1. DIAMOND or MMseqs2 (translated nucleotide-to-protein search) -- highest leverage

Translated search helps with two problems at once:

- **Ultima's indel errors**: A single-base indel in a coding region shifts the
  reading frame for a few codons, but most of the protein alignment stays intact.
  At the nucleotide level, that same indel breaks kmer matching and degrades
  alignment scores across the entire read.
- **Divergent virus detection**: Protein sequences are more conserved than
  nucleotide sequences. Translated search detects viruses at ~40-60% nucleotide
  identity where minimap2 finds nothing. This isn't Ultima-specific, but it
  compounds with the indel tolerance.

**Where it would slot in**: Supplement (not replace) the minimap2 viral alignment
at Step 5. After depletion, run both minimap2 and DIAMOND against a viral protein
database, then merge hits.

**Why not for this pilot**: It would require a viral protein database (not just
nucleotide genomes), a new DIAMOND module, and a merge step. More importantly, it
changes what we're measuring -- differences could be attributed to the translated
search rather than the sequencing platform. If added to the Ultima pipeline, you'd
want to add it to Illumina too for a fair comparison, at which point it's a pipeline
improvement project, not an Ultima adaptation.

**Estimated improvement**: 10-30% more viral reads recovered, concentrated in
divergent viruses and reads with homopolymer indels in coding regions.

**Future relevance**: If the goal shifts from "evaluate the platform" to "maximize
viral detection from Ultima data for production," DIAMOND is the clear first addition.

### 2. sourmash or Mash (MinHash-based pre-screening) -- moderate leverage

BBDuk's kmer screen is the hard gate in the middle path -- any viral read that fails
kmer matching is permanently lost. With k=21 and hdist=1, BBDuk tolerates substitutions
and single-base indels, but multi-base homopolymer indels (Ultima's signature error)
break all overlapping kmers. MinHash sketching is more robust because it samples a
sparse subset of kmers and uses Jaccard similarity rather than requiring a minimum
count of exact matches.

**Practical issue**: sourmash/Mash are usually used for genome-level comparison, not
read-level screening. Per-read screening of 333M reads may not be as fast as BBDuk.

**Estimated improvement**: ~2-5% more viral reads passing the pre-filter. Modest,
because BBDuk with relaxed params already recovers most reads. The real fix if BBDuk
sensitivity is a concern is to skip BBDuk entirely (no new tool, just more compute).

### 3. Ultima Aligner (UA) -- low leverage for metagenomics

UA is a BWA derivative (available as `ultimagenomics/alignment` on Docker Hub) that
incorporates Ultima's flow-space error model. It understands that errors are
non-uniform: a homopolymer of length 6 has a different error probability than one of
length 3, and this depends on the reference context. For human WGS, this produces
better variant calls.

**For metagenomics, the benefit is limited because:**

- **The flow model needs accurate reference context.** UA's advantage comes from
  knowing the reference base composition around each position. When aligning to a
  diverse viral database with 30-40% sequence divergence, the reference context is
  uncertain. The flow-space correction is less informative when the reference itself
  is approximate.
- **minimap2 `-ax sr` already handles indels well generically.** Its gap-affine
  chaining was designed for indel-heavy error profiles. It doesn't model flow-space,
  but it aligns through indels effectively.
- **Proprietary index format (.uai).** Requires building UA indexes for viral, human,
  and contaminant databases -- new infrastructure with no reuse for Illumina or ONT.
- **No metagenomics benchmarks.** All published UA validations are on human WGS/WES
  and clinical samples.

**Where UA might help**: Reads with long homopolymer stretches (6+ bases) aligning to
closely related references (>95% identity). A small fraction of metagenomic reads.

**Estimated improvement over minimap2 `-ax sr`**: 1-3%, concentrated in
homopolymer-rich regions of well-characterized reference genomes.

### 4. Other Ultima Docker Hub tools (31 images total, 3 potentially relevant)

- **`ultimagenomics/sorter`** (flow-aware dedup): UMI-aware duplicate marking adapted
  for single-end variable-length reads. Interesting concept, but dedup is not critical
  for metagenomics (unlike WGS where PCR duplicates inflate coverage). The middle path
  doesn't include dedup, and that's appropriate.
- **`ultimagenomics/trimmer`**: Already applied by the sequencing core before we
  received the data. Redundant.
- **`ultimagenomics/star`**: RNA-seq splice-aware aligner. Our data is RNA-seq, but
  it's untargeted metagenomics -- most reads are from microbial genomes, not spliced
  host transcripts. Would require splice-junction databases for thousands of viral
  genomes. Not practical.

The remaining ~28 images are variant calling tools (DeepVariant rewrites, GATK
flow-mode, somatic/germline CNV, SV detection, HLA typing, pharmacogenomics, etc.)
with no relevance to metagenomic sequencing.

---

## Summary: Gap Analysis for the Middle Path

| Gap | Estimated size | Best fix | New tool needed? |
|---|---|---|---|
| BBDuk missing viral reads with multi-base homopolymer indels | ~2-5% of viral reads | Skip BBDuk (more compute) or sourmash | No (skip BBDuk) or Yes (sourmash) |
| minimap2 missing divergent viruses at nucleotide level | ~5-15% at high divergence | DIAMOND translated search | Yes |
| minimap2 suboptimal scoring in long homopolymer regions | ~1-3% of reads | Ultima Aligner | Yes |

**Total potential improvement from new tools: ~10-20% more viral reads at the upper
bound**, but most of that comes from DIAMOND's ability to detect divergent viruses --
which is a general pipeline improvement, not an Ultima-specific adaptation.

For the pilot's goal of evaluating Ultima as a sequencing platform, the middle path
without new tools provides a clean comparison where the primary variable is the
sequencing chemistry.
