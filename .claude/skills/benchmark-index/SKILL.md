---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so it can annotate transitions with existing rule coverage, classify lost / gained genome IDs by reason, and check Kraken2 / SILVA reference freshness; the human/taxonomy/VHDB URLs are not auto-checked), then fills `review-template.md` with data from the script's `summary.md` to produce `REVIEW.md`. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

The point of the report is to help the reviewer decide whether to ship this index as-is or change something first. The script does the data work; you fill in `review-template.md` from the script's `summary.md`.

**`review-template.md` is the source of truth for report structure.** Open it, read it, fill it in literally. This skill provides procedural support (how to run the script, where to look up things) and a small glossary of script-specific category labels — it does **not** re-describe what each section should contain. When the template and the skill seem to disagree, the template wins.

## When to use

- The user wants to vet a new index release before promoting it to production.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no written review), just run the script and surface `summary.md` — don't write `REVIEW.md`.

## Inputs

- `--old <root>`: parent of `output/` for the old (reference) index. `s3://...` or local path. **Required.**
- `--new <root>`: parent of `output/` for the new (target) index. **Required.**
- `--out <dir>`: output directory. Use an absolute path so paths in the report are reader-portable. **Required.**
- `--repo-root <path>`: a mgs-workflow checkout. Optional in the script, but in practice always pass `--repo-root .` from a checkout — without it the script skips the coverage / redistribution / categorization annotations that drive the report, and the Edge cases section becomes load-bearing.

If `--old`, `--new`, or `--out` is missing, ask the user — don't guess.

## Procedure

### Step 1 — Run the script

`cd` into a mgs-workflow checkout, then:

```bash
cd /path/to/mgs-workflow
python bin/benchmark_index.py \
  --old <old> --new <new> \
  --out <outdir> --repo-root .
```

Use **absolute** paths for `--out` (e.g. `/tmp/bench-...`). Takes ~60 seconds. If `<outdir>/summary.md` already exists, reuse it — but note that to the user.

### Step 2 — Read `summary.md`

The script writes `<outdir>/summary.md`, structured to mirror `review-template.md` 1:1. Every table the template asks for is already populated; every `**Findings:**` block has script-generated factual bullets. Read it end-to-end.

### Step 3 — (Optional) VHDB cross-reference

Only needed when you're considering a candidate change for a 0→1 promotion or an override policy gap. Fetch VHDB once:

```bash
curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$6"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
```
Columns: virus tax id, virus name, disease, host tax id, host name.

Notes:
- Homo sapiens taxid is 9606. If it's in column 4, VHDB lists this virus as human-infecting.
- If the taxid returns no rows, try grepping by name — freshly-minted NCBI species IDs often lag VHDB's indexing, and the parent virus may carry annotations under a legacy taxid.
- Host taxid `1` (`root`) in column 4 means VHDB has the species but no specific host annotation; the demotion is real and no override is warranted on this evidence alone.

### Step 4 — Fill in the template

Copy `review-template.md` to `<outdir>/REVIEW.md` and fill it in. **Read the template's instructions for each section and follow them literally.** Use `summary.md` as the data source.

Two reminders that aren't obvious from the template alone:

- **Trust the script's annotations.** If a row has `covered_by_hard_exclude = 2169574`, the script has verified the lineage; don't manually re-classify. The "Categorization buckets" glossary below explains what each script-emitted label means.
- **Err on the side of inclusion in §Recommendations.** Every plausible candidate change should appear, even at `low` confidence — the reviewer is better placed to dismiss a noisy recommendation than to spot one you didn't surface. In particular: any stale reference in §1 Findings, every uncovered promotion in §4, and every override policy gap in §4 should each appear as a candidate change.

### Step 5 — Hand off

Print the `REVIEW.md` path back to the user. Don't open a PR or commit anything — recommendations need human judgment before going to code review.

Don't `rm -rf` the outdir between iterations; the script overwrites its own files, but `REVIEW.md` would go with the directory.

## Categorization buckets (glossary)

These are the labels the script writes into `genomes_lost_categorized.tsv`, `genomes_gained_categorized.tsv`, and the §3.1 counts in `summary.md`. The template uses them as the §3.1 row labels; this glossary explains the semantics.

**Lost gid categories** (priority order; each gid gets the first applicable bucket). The `raw` categorizer recovers each lost genome's *build-time* taxid + assembly_status by joining its `assembly_accession` into the target index's `virus-genome-metadata-raw.tsv.gz` (the pre-filter assembly list), so attribution is exact — no NCBI lookups, no no-drift assumption. Assembly-lifecycle reasons are checked before taxonomy/policy reasons, so the policy buckets mean "current assemblies we stopped surveilling" and aren't inflated by routine version churn:

- `absent_from_ncbi`: the lost genome's assembly is absent from the target index's raw metadata entirely — NCBI suppressed or removed the assembly between builds, so there's no build-time assignment to attribute anything else to. (Suppressed-vs-removed isn't sub-split; it would need per-accession NCBI lookups.)
- `non_current_genome_version`: assembly present in raw but its `assembly_status` is not `current` (superseded, or suppressed-but-still-listed) — dropped by the `assembly_status == 'current'` filter. Read directly from the raw table, not inferred.
- `hard_excluded`: build-time species_taxid (or ancestor) is in `viral_taxids_exclude_hard` in the new build.
- `reassigned_to_excluded`: build-time species_taxid differs from the old species_taxid AND the new species is no longer surveilled — the genome's taxid moved (NCBI/ICTV restructure) to a taxon that fails the host-infection screen.
- `infection_status_demotion`: build-time species_taxid equals the old species_taxid AND the species is no longer surveilled — an upstream VHDB host-annotation change demoted the taxon.
- `other`: present, current, surveilled — yet absent from the new gid set. Should be ~0; if not, flags a downstream/sequence-level drop (`genome_patterns_exclude`, masking, dedup) worth investigating.

The target index must publish `virus-genome-metadata-raw.tsv.gz` (the pre-filter assembly list). If it's missing, the script errors out rather than guessing — rebuild the index with a pipeline version that emits it.

**Gained gid categories** (priority order). Keyed on the genome's assigned (leaf) taxon, using the assembly's `release_date` + `source_database` (from the raw table) and the old annotated DB — no NCBI lookups. Genome-lifecycle reasons (is the genome itself new?) are checked before taxonomy/policy reasons:

- `newly_deposited`: assembly `release_date` is after the old index build — genuinely deposited to NCBI since the previous build.
- `hard_included`: the genome's new leaf taxon (or ancestor) is in `ref/host-infection-overrides.json`.
- `new_taxon_in_taxonomy`: the genome's new leaf taxon did NOT exist in the old taxonomy DB — a taxon NCBI/ICTV minted between builds.
- `infection_status_promotion`: the leaf taxon was in the old DB but not surveilled then and is now — its lineage's infection status flipped 0→1.
- `pre_existing_reincluded`: the assembly predates the old build and its taxon was already eligible, yet it wasn't in the old surveillance set — so a change in the index's *inclusion config* surfaced it (not new biology). The classic case is an `assembly_source` switch (GenBank-only → all) pulling in RefSeq; a hard-exclude removal is another. Empty in steady state, so frequently the *largest* gained bucket only when config changed — don't read a big number here as new sequence data. The §3.3 findings break it down by `source_database` and name the responsible params change, so the driver is data-driven, not assumed.
- `no_release_date`: an eligible-taxon assembly with no `release_date`, so the newly-deposited-vs-pre-existing split can't be made. Checked last, so a missing date never pre-empts a release-date-independent reason (`hard_included` / `new_taxon_in_taxonomy` / `infection_status_promotion`) — those still apply. Named explicitly rather than as a catch-all; should be ~0. (Unlike the lost side, the gained tree has no `other` bucket: every gained genome resolves to one of the reasons above.)

Both lost and gained categorizers key on the genome's assigned **leaf** taxon, never a species-rank rollup; the species rollup appears only inside the surveillance predicate (leaf-positive OR species-positive), mirroring `filter_viral_genbank_metadata.py`.

## Edge cases

- **`--repo-root` skipped**: the script falls back to no coverage / categorization annotation. Re-run with `--repo-root .` from a mgs-workflow checkout.
- **A reference check errors out** (network blip): summary.md §1 shows `status: error` for that row. Note the inability to verify in §1 Findings and continue.
- **Empty `genomes_added` / `genomes_removed`**: §3.1 categorized counts will all read 0; §3.2 / §3.3 collapse to "No genome IDs lost/gained".
