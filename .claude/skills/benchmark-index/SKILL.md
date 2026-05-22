---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so it can annotate transitions with existing rule coverage, classify lost / gained genome IDs by reason, and check reference-DB freshness), then fills `review-template.md` with data from the script's `summary.md` to produce `REVIEW.md`. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
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
- `--out <dir>`: output directory. Use an absolute path so paths in the report are reader-portable.
- `--repo-root <path>`: a mgs-workflow checkout. Without this, the script falls back to plain counts; with it you get the coverage / redistribution / categorization annotations that drive the report.

If any of these is missing, ask the user — don't guess.

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

**Lost gid categories** (priority order; each gid gets the first applicable bucket):

- `hard_excluded`: gid's old species_taxid (or ancestor) is in `viral_taxids_exclude_hard` in the new build. The new exclude rule drops the gid at the filter stage.
- `change_in_assigned_taxid`: gid's old species had its gids redistributed to a different species_taxid (NCBI/ICTV restructure); the gid was dropped under the new assignment.
- `infection_status_change`: gid's old species lost ALL its gids in new metadata AND wasn't redistributed. The species's `infection_status` filter result flipped (could be `human`, `vertebrate`, or any host the filter checks — not just human), so the species drops out of the surveillance set.
- `non_current_genome_version`: gid is gone but its species_taxid still has surviving gids in new metadata. Almost always the `assembly_status == 'current'` filter dropping a superseded NCBI assembly version.
- `other`: no rule applies. Should be rare.

**Gained gid categories** (priority order):

- `hard_included`: gid's new species_taxid (or ancestor) is in `ref/host-infection-overrides.json`. The include rule explains why this gid lands in the surveillance set.
- `change_in_assigned_taxid`: gid's new species_taxid is the destination of an NCBI/ICTV restructure (some old species had its gids redistributed here). The taxid changed and the new assignment passes the filter.
- `newly_deposited_existing`: gid's new species was already in old metadata (the species was already surveilled). New data for an already-tracked species.
- `infection_status_change`: gid's new species was in the old taxonomy DB but NOT in old metadata (the species existed but failed the old filter); now it's in new metadata. The filter result flipped — the species is newly in the surveillance set.
- `new_species_in_taxonomy`: gid's new species_taxid did NOT exist in the old taxonomy DB and isn't a redistribution destination — a brand-new species concept NCBI/ICTV added between builds.
- `other`: should be effectively empty under the categorization above. If anything lands here, treat as a bug.

## Edge cases

- **`--repo-root` skipped**: the script falls back to no coverage / categorization annotation. Re-run with `--repo-root .` from a mgs-workflow checkout.
- **A reference check errors out** (network blip): summary.md §1 shows `status: error` for that row. Note the inability to verify in §1 Findings and continue.
- **Empty `genomes_added` / `genomes_removed`**: §3.1 categorized counts will all read 0; §3.2 / §3.3 collapse to "No genome IDs lost/gained".
