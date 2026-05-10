# Example: replace a pipeline step with a benchmark

A worked PR description for a structural pipeline change with a backwards-compat impact and meaningful performance evidence. Original PR: [#766 — Replace BBDuk viral k-mer pre-screen with Nucleaze](https://github.com/securebio/nao-mgs-workflow/pull/766).

## What this example demonstrates

- **Summary ordered by reviewer priority.** First three paragraphs of the body, in order: (1) the core change and *why it's useful*, (2) the backwards-compatibility break stated *with explicit direction* (which combinations are still valid, which aren't, and which version field changes), (3) the sensitivity tradeoff stated up front rather than buried under the benchmark tables. A reviewer should be able to greenlight or push back from those three paragraphs alone.
- **Single bullet at the end of `## Changes`** notes test coverage in one sentence, not as a separate "Tests run" or "Test plan" section. Per `CLAUDE.md`: don't include a test plan.
- **Performance evidence as side-by-side tables, not prose-encoded percentages.** Three tables — process / subworkflow / final hits — each scoped to one comparison. Each table is followed by one short paragraph that interprets it (e.g. "the −63 % CPU-h saving comes almost entirely from the kmer step itself").
- **Honest framing of noise vs. signal.** The `BOWTIE2_VIRUS` paragraph deliberately doesn't construct a causal story for an N=4 outlier — it states the observation and notes "with N=4 this could just be sample-level noise rather than a systematic effect of the swap." Earlier drafts of this PR proposed two different mechanisms; both turned out to fail under scrutiny, and the right framing was the agnostic one.
- **Flat header structure.** Two top-level sections (`# Summary`, `# Benchmarking`) with one level of subsections each. No `## Container > ### Subsection > ### Subsection` nesting. Each header signals exactly what's in the section.
- **Test rig spelled out** at the top of `# Benchmarking` so every number that follows can be located in time and resources (`8-core / 15 GB sandbox, maxForks=1, pre-staged inputs, pre-built index excluded by design`). Otherwise the numbers are uninterpretable.

## The PR body

The verbatim body of [PR #766](https://github.com/securebio/nao-mgs-workflow/pull/766) follows. Keep this in sync if the PR's body is materially edited post-merge.

---

# Summary

Replaces the per-sample BBDuk viral k-mer pre-screen in `EXTRACT_VIRAL_READS_SHORT` with [Nucleaze](https://github.com/jackdougle/nucleaze), which consumes a pre-built binary k-mer index now produced once by INDEX. This factors the k-mer datastructure out of the per-sample hot path (BBDuk rebuilds its hash on every sample of every run), and the screen tool itself is faster, resulting in large performance improvements.

The addition of a new index file for Nucleaze (`virus-genomes-masked.nucleaze.bin`) breaks backwards compatibility in one direction: old indexes cannot be used by this version of the pipeline, so `pipeline-min-index-version` is bumped to `3.2.1.5`. Indexes built by this version remain usable by older pipelines, which simply ignore the new file (`index-min-pipeline-version` unchanged).

Nucleaze is not a full drop-in replacement for BBDuk, due to the presence of the latter's `mm=t` option, which wildcards the middle base of each kmer. Conversely, Nucleaze does strict 24-mer matching, resulting in a more conservative filter. At the level of the individual process, the effect of this can be large, with Nucleaze returning less than half as many matching reads as BBDuk. However, the great majority of these are false positives that are filtered out by downstream stages of the pipeline, resulting in much smaller effects at the level of the subworkflow: on a 4-sample spot check, the final number of viral hits dropped by 0.77%, all from common enteric viruses. Nevertheless, this is sufficient to break results compatibility and necessitate a third-number version bump.

The BBDuk-based ribosomal screen in `PROFILE` is intentionally untouched — Nucleaze does not (yet) support a fraction-based threshold equivalent to BBDuk's `minkmerfraction`.

## Changes

- INDEX: `MAKE_VIRUS_INDEX` now also runs `NUCLEAZE_INDEX`, publishing `virus-genomes-masked.nucleaze.bin` next to the masked viral FASTA. New `nucleaze_k` param (default `24`).
- RUN: `EXTRACT_VIRAL_READS_SHORT` swaps `BBDUK_HITS_INTERLEAVE` for `NUCLEAZE`, reading `virus-genomes-masked.nucleaze.bin` from the ref dir.
- `NUCLEAZE` module streams output through two `pigz -p ${task.cpus} -1` workers via named FIFOs, with explicit `wait $PID` so the script doesn't exit before pigz drains the pipes. `pigz -1` is fast enough that nucleaze runs at native speed (no pipe back-pressure); using default `-6` slows nucleaze by 3×. Adds `pigz` to the rust-tools image.
- Removes the now-unused `BBDUK_HITS_INTERLEAVE` process and its dedicated module test (`tests/modules/local/bbduk/bbduk_hits.nf.test`). The fraction-based `BBDUK` process used by `PROFILE` is retained.
- Internal renames (no user-facing impact): `bbduk_match`/`bbduk_trimmed` → `kmer_match`/`kmer_trimmed` on the `EXTRACT_VIRAL_READS` / `EXTRACT_VIRAL_READS_SHORT` emit channels; `min_kmer_hits`/`bbduk_suffix` → `minhits`/`kmer_suffix` on the params map.
- Doc updates in `docs/run.md` and `docs/output.md` to reflect the new tool and the new published index file.

All existing module, subworkflow, INDEX and RUN nf-tests pass. `tests/workflows/run.nf.test.snap` is byte-identical to the BBDuk implementation on the tiny test data, since at small scale the `mm=t` extras don't survive Bowtie2 anyway.

# Benchmarking

All numbers below are from a single 8-core / 15 GB EC2 sandbox, with `process.maxForks = 1` so only one task runs at a time (no cross-sample CPU contention). Inputs are pre-staged on local disk; index is pre-built (the one-time `NUCLEAZE_INDEX` cost is excluded by design — it amortizes across all RUN invocations).

## Process: BBDuk vs Nucleaze on the k-mer screen alone

19 samples from the Illumina-100M benchmark dataset, processed in series.

| Metric | BBDuk (dev, `mm=t` default) | Nucleaze (this PR) | Δ |
|---|---:|---:|---:|
| Total wall time | 1107.6 s | 309.8 s | **−72.0 %** |
| Total CPU-hours | 2.163 | 0.415 | **−80.8 %** |
| Per-sample wall (mean) | 58.3 s | 16.3 s | **−72.0 %** |
| Per-sample CPU-hours (mean) | 0.114 | 0.022 | **−80.8 %** |
| Peak RSS (per sample) | 7.4–8.7 GB | 1.29 GB | **−85 %** |
| Total emitted match reads | 541,566 | 249,634 | −53.9 % |

Wall and CPU savings are remarkably consistent across samples (70–75 % wall and 78–86 % CPU-h on every one). The match-count drop varies more (−14 % to −77 % per sample) and is the upper bound on the sensitivity impact — most of those reads are filtered by Bowtie2 downstream.

## Subworkflow: whole `EXTRACT_VIRAL_READS_SHORT`

4 samples through the full subworkflow (all 18 processes per sample), `process.maxForks = 1` so per-sample timings are clean of cross-sample CPU contention.

| Sample | Dev wall | PR wall | Δ wall | Dev CPU-h | PR CPU-h | Δ CPU-h |
|---|---:|---:|---:|---:|---:|---:|
| CARiverside_20250324 | 7.19 min | 6.20 min | −13.7 % | 0.1445 | 0.0523 | −63.8 % |
| CARiverside_20250626 | 6.99 min | 7.66 min | +9.5 % | 0.1676 | 0.0702 | −58.1 % |
| CHI-A_20250727 | 7.66 min | 5.89 min | −23.1 % | 0.1469 | 0.0500 | −65.9 % |
| MO_Milan_20250813 | 7.09 min | 6.42 min | −9.5 % | 0.1418 | 0.0479 | −66.2 % |
| **Aggregate** | **28.92 min** | **26.16 min** | **−9.6 %** | **0.6008** | **0.2204** | **−63.3 %** |

The **−63 %** aggregate CPU-hours saving comes almost entirely from the kmer step itself: `BBDUK_HITS` cost 1700 cpu-s across the 4 samples vs 334 cpu-s for `NUCLEAZE`, and the rest of the 18 processes net out to roughly the same compute on both branches. The **−10 %** aggregate wall saving is similarly dominated by the kmer-step difference (237 s → 61 s of subworkflow wall): downstream wall is roughly flat, because `BOWTIE2_OTHER` (which aligns surviving reads against a 17 GB other-contaminants index) takes ~5 min per sample regardless of upstream filter strictness and accounts for ~70 % of subworkflow wall on both branches.

`BOWTIE2_VIRUS` aggregates +28 s wall on PR despite a smaller input, but per-sample variance is large (one sample +384 %, one −71 %, two roughly flat) and the absolute size is small (10–50 s tasks); with N=4 this could just be sample-level noise rather than a systematic effect of the swap.

## Final viral hits TSV

4 samples through `EXTRACT_VIRAL_READS_SHORT`; comparing the published `hits_final` per branch.

| Sample | Dev hits | This PR hits | Lost | % lost |
|---|---:|---:|---:|---:|
| CARiverside_20250324 | 5,146 | 5,125 | 21 | 0.41 % |
| CARiverside_20250626 | 2,931 | 2,902 | 29 | 0.99 % |
| CHI-A_20250727 | 934 | 923 | 11 | 1.18 % |
| MO_Milan_20250813 | 737 | 723 | 14 | 1.90 % |
| **Aggregate** | **9,748** | **9,673** | **75** | **0.77 %** |

Every PR hit is also a dev hit (no reads gained, only lost). The lost reads are dominated by enteric viruses already represented many times over in the surviving hits (Mamastrovirus, Sapovirus, Aichivirus, Hepatitis D virus 1, multiple Norovirus genogroups), with high-confidence Bowtie2 alignment scores (median 140–160, well above the production threshold of 20). Two of the four samples drop one rare-Norovirus-variant detection that had only a single supporting read in dev; Norovirus is highly abundant in wastewater so these aren't load-bearing.

Generated with [Claude Code](https://claude.com/claude-code)
