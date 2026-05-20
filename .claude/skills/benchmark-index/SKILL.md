---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` to generate the underlying TSVs, then interprets the numbers — flags shrunk DBs, names species-level 1→0 infection-status demotions (cross-referenced against the current Virus-Host-DB to attribute the change to upstream drift vs a workflow change), spot-checks dropped genomes against the residual per-species coverage, scans 0→1 promotions for false-positive phage classes, and proposes concrete config edits (additions to `ref/host-infection-overrides.json` or `viral_taxids_exclude_hard`) keyed to the findings. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

Compare an `--old` and `--new` index release, produce a structured markdown review that a maintainer can paste into a Slack thread / PR description / Linear ticket. The script (`bin/benchmark_index.py`) handles the deterministic data extraction; this skill handles the interpretation that's worth doing every time but not worth a reviewer rediscovering from scratch.

## When to use

- The user wants to vet a new index release before promoting it to production.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no interpretation), just run the script — don't write the review.

## Inputs

- `--old <root>`: parent of `output/` for the old index. `s3://...` or a local path. **Required.**
- `--new <root>`: parent of `output/` for the new index. **Required.**
- `--out <dir>` (optional): where to land the script's TSVs. Default `./bench-<old-tag>-vs-<new-tag>/` derived from the URI basenames.

If either root is missing, ask the user. Don't guess.

## Procedure

### Step 1 — Run the script (skip if outputs already exist)

```bash
python bin/benchmark_index.py --old <old> --new <new> --out <outdir>
```

If `<outdir>` already contains `sizes.tsv`, `summary.md`, and the per-host `infection_status_changes_*.tsv` files, skip and reuse. Re-running is cheap (a few small file downloads) but flag to the user that you reused existing outputs.

Read `<outdir>/summary.md` first for the top-line counts; use them as anchors when writing the report.

### Step 2 — Interpret each TSV

For each file, do the specific filter / pivot called out below, then write the findings into the report (Step 4). Don't paste raw TSV — name the specific organisms / taxa / DBs.

**`sizes.tsv`** — sort by `delta_bytes`. Any row with negative `delta_bytes` is *prima facie* a regression to investigate. Common explanations:
- `virus-genomes-masked.fasta.gz` shrinking + `virus-genome-metadata-gid.tsv.gz` shrinking together → assembly-status filter tightened (e.g., dropping superseded NCBI assemblies). Check the workflow CHANGELOG.
- A bowtie2 / minimap2 index identical to the previous version → the upstream reference URL is unchanged. Normal.
- `core_nt` growing > 10% → normal NCBI BLAST growth.

**`genomes_by_species.tsv`** — sort by `|delta|` desc, focus on negative deltas. For each species with a meaningful loss, check `new_count`:
- `new_count > 0`: species still covered. Note the count drop and move on.
- `new_count == 0`: species is *gone* from the genome FASTA. **Flag.** Look up `organism_name`; if it's a known human / vertebrate pathogen, this is a real concern and needs a config-level mitigation (re-include via `download_virus_taxid` or similar).

**`infection_status_changes_human.tsv`** (and the other hosts, but `human` is the most actionable) — filter to:
- `rank == "species"` AND `old_status == "1"` AND `new_status == "0"` → **species-level demotions**, the most actionable finding. Name each one. Note their pathogenic status from background knowledge (e.g. LCMV, Puumala virus, Banzi virus, dengue, etc.). These are the candidates for `ref/host-infection-overrides.json`.
- `old_status == "0"` AND `new_status == "1"` → **promotions**. Skim for suspicious patterns:
  - Microviridae / Microvirus / gokushovirus / phage names → false positives, candidates for `viral_taxids_exclude_hard`.
  - Smacoviridae / Picobirnaviridae → same.
  - Anything labelled `Human gut <foo>` → likely a phage from human stool samples, false positive.

Don't editorialise on `2 ↔ 3` or `1 → 3` transitions unless the user asks; those are usually upstream-VHDB rephrasings.

**`genomes_removed.tsv`** — skim the `organism_name` column. Pattern-match against well-known pathogens (SARS-CoV-2, influenza A/B/C, RSV-A/B, HMPV, parainfluenza 1-4, hCoV 229E/HKU1/NL63/OC43, dengue, Zika, West Nile, Ebola, Marburg, Lassa, mpox). For each match, cross-check against `genomes_by_species.tsv` — usually a known pathogen has thousands of remaining genomes, so the drop is just assembly culling.

**`taxa_added.tsv` / `taxa_removed.tsv`** — usually high cardinality (thousands of taxa) and driven by NCBI taxonomy churn. Skim for surprising patterns (e.g., a whole family disappearing) but don't dump the list.

**`params_diff.txt`** — look for:
- `kraken_db` URL change → note the date version (e.g., `k2_standard_20250714` → `k2_standard_20251015`).
- Workflow pipeline-version change → call out the version delta, link to the workflow CHANGELOG between those versions.
- New / removed `params.*` keys → note them as configuration surface changes.
- Path prefix changes (e.g., `/home/ec2-user/mgs-workflow/repo/ref/` → `/home/ec2-user/mgs-workflow/ref/`) → cosmetic, ignore.

### Step 3 — Cross-reference

For each species-level **1→0 human demotion** identified in Step 2:

1. Fetch the current upstream Virus-Host-DB:
   ```bash
   curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
   ```
2. For each demoted `<taxid>`, run:
   ```bash
   awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
   ```
   - If Homo sapiens (taxid 9606) appears in column 8 → the demotion is **not** upstream; investigate the workflow code.
   - If Homo sapiens doesn't appear but the disease column ($6/$7) names a human disease → the demotion **is** upstream VHDB drift; the host annotation isn't capturing what the disease annotation does. This is the typical pattern.

For each promoted family/class that looks like a false-positive bacteriophage:

1. Check current `viral_taxids_exclude_hard` in `configs/index.config`:
   ```bash
   grep viral_taxids_exclude_hard configs/index.config
   ```
2. If the class/family taxid isn't already in the list, the recommendation is to add it.

For each species-level demotion candidate:

1. Check current `ref/host-infection-overrides.json`:
   ```bash
   python3 -c "import json; print([o['taxid'] for o in json.load(open('ref/host-infection-overrides.json'))['overrides']])"
   ```
2. If the taxid isn't already overridden, the recommendation is to add it.

### Step 4 — Produce the report

Write a markdown report to `<outdir>/REVIEW.md` (and surface it inline in the user-facing reply). Five sections, in order:

```markdown
# Index benchmark review: <OLD> → <NEW>

## 1. Per-DB sizes

[Bullet per DB that changed materially. Flag any that shrunk and explain.
Reference sizes.tsv.]

## 2. Virus taxonomy & infection status

[Per-host bullets summarising the transition counts (1→0, 0→1, etc.).
Then a sub-bullet listing the species-rank 1→0 human demotions by name,
with the VHDB cross-reference outcome from Step 3. Then a sub-bullet
listing the suspicious 0→1 promotions (likely phages).]

## 3. Lost virus genomes

[Top-line: N genomes added, M removed. Then a sub-bullet listing species
that lost the most genomes; for each, note whether they still have
coverage (new_count > 0) and whether they're a human pathogen.]

## 4. Other notable changes

[Params diff highlights: kraken version, pipeline version, new/removed
params. Schema changes (e.g., metadata column-set differences).
New output files.]

## 5. Recommendations

[Concrete config edits, keyed to findings:
- "Add <taxid> (<name>) to ref/host-infection-overrides.json: <reason>"
- "Add <taxid> (<name>) to viral_taxids_exclude_hard: <reason>"
- "Investigate workflow change <X> (CHANGELOG link) — this likely
   explains <observation>"
For each recommendation, link to the file/line where the edit goes.
If no recommendations are warranted, say so explicitly.]
```

### Step 5 — Hand off

Print the report path and a 3-line summary to the user. Don't open a PR or commit anything — the recommendations need human judgment before they're committed.

## What not to do

- **Don't fabricate**: If the data doesn't show something, say so. Don't claim a species is "human-infecting" unless you can name a disease or a known prior context.
- **Don't paste raw TSVs**: The user can `less` the files themselves; the report's job is to name the specific findings.
- **Don't dump every status change**: Filter to species rank for the actionable demotions; aggregate the rest as counts.
- **Don't act on the recommendations**: Surface them; let the user decide. Config edits go through code review.
- **Don't loop the cross-reference**: If VHDB has 40K rows, don't fetch it 40 times — fetch once at the top of Step 3 and grep against the local file.

## Edge cases

- **No infection-status changes for a host**: Skip the bullet for that host rather than writing "no changes."
- **Schema change in `virus-genome-metadata-gid.tsv.gz`** (column set differs): the script's `diff_genome_metadata` already restricts to common columns; flag the schema difference in Section 4.
- **One-off staging failures** (network blip on `aws s3 cp`): retry the script before falling back to a partial report.
- **Old index missing a host group** (e.g., `infection_status_bird` only added later): note in Section 2 that the host is newly tracked; no transition data to report.
