---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so it can annotate transitions with existing rule coverage, classify lost / gained genome IDs by reason, and check reference-DB freshness), then maps the script's `summary.md` onto the structure in `review-template.md` to produce a written `REVIEW.md`. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

The point of the report is a **ship-or-regen decision about this specific index**. Every finding maps to one of two outcomes: (a) the change is acceptable and the index can be released as-is, or (b) the index needs regenerating with a specific config change applied. **Never defer to a "next build"** — if a fix is needed, the recommendation is to regenerate this index with the fix.

`bin/benchmark_index.py` does the heavy data work — it diffs sizes, content (FASTA records/bp, TSV row counts, metadata schema), infection-status transitions (annotated with `covered_by` / `included_for_other_hosts` / `driven_by_genome_loss` / `cross_host_actionable_on`), lost / gained genome IDs (each classified by the categories described below), lost-species inventories (annotated with `covered_by_hard_exclude`, `redistributed_to_species_taxid`, `redistributed_genome_count`, `truly_lost_count`), and reference-DB freshness (active checks for Kraken2 and SILVA). The script writes a self-contained `summary.md` structured around the same sections as `review-template.md`.

Your job is to turn `summary.md` into `REVIEW.md` using `review-template.md` as the authoritative structure. The script populates every Findings section and table with factual data; you add prose interpretation in the `Summary`, per-section `Findings:` bullets where they need fleshing out, `§3.2 / §3.3` discussion, and `Recommendations`. **Don't invent your own report structure** — copy the template's section order, headings, and table shapes exactly.

## When to use

- The user wants to vet a new index release before promoting it to production.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no written review), just run the script and surface `summary.md` — don't write `REVIEW.md`.

## Inputs

- `--old <root>`: parent of `output/` for the old (reference) index. `s3://...` or local path. **Required.**
- `--new <root>`: parent of `output/` for the new (target) index. **Required.**
- `--out <dir>`: output directory. Use an absolute path so paths in the report are reader-portable.
- `--repo-root <path>`: a mgs-workflow checkout. Without this, the script falls back to plain counts; with it you get all the coverage / redistribution / categorization annotations that drive the report.

If any of these is missing, ask the user — don't guess.

## Procedure

### Step 1 — Run the script

`cd` into a mgs-workflow checkout first, then:

```bash
cd /path/to/mgs-workflow
python bin/benchmark_index.py \
  --old <old> --new <new> \
  --out <outdir> --repo-root .
```

Use **absolute** paths for `--out` (e.g. `/tmp/bench-...`, not `./bench-...`) — they'll appear in the report and a colleague should be able to copy them verbatim.

Takes ~60 seconds. If `<outdir>/summary.md` already exists, skip the script run and reuse — but note that to the user.

### Step 2 — Read `summary.md` end-to-end

`summary.md`'s sections map 1:1 onto `review-template.md`. Every table the template asks for is already populated; every `**Findings:**` block has script-generated factual bullets. Your job in Step 4 is to add prose interpretation, write a tight Summary, decide §3.2 / §3.3 discussion priorities, and synthesize Recommendations as ship-or-regen decisions.

Section map:

| `summary.md` section | What's pre-populated | What you add in REVIEW.md |
|---|---|---|
| §1 Staleness | Active check for Kraken2 + SILVA, passive URLs for human/taxonomy/VHDB; Findings bullets per stale ref. | Decision framing: ship as-is or regenerate against the newer reference. |
| §2 Database size | Compressed-bytes table + Findings bullets for FASTA records/bp deltas, TSV row counts, schema diff column names. | Prose interpretation when a compressed shrink hides content growth, or when a schema change drives bytes. |
| §3.1 Total | Categorized count bullets for lost + gained gids (see "Categorization buckets" below). | Nothing — copy verbatim. |
| §3.2 Losses | Top-5 species per loss category + summary of species-dropped-to-zero (with redistribution / true-loss split). | Prose discussion: which categories matter, which are routine. **When the `other` bucket includes routine surveillance pathogens (SARS-CoV-2, RSV, influenza, etc.), cross-reference `CHANGELOG.md` for assembly-status filter changes** — these losses are typically the `non_current_genome_version` pattern. If the script puts them in `other` instead, surface a pulled-out sub-bullet for them; don't leave a shared-fate group in `other`. |
| §3.3 Gains | Top-5 species per gain category. | Same shape as §3.2: prose discussion. If a meaningful sub-group lives in `other`, surface it as its own bullet. |
| §4 Infection status | Per-host promotion / demotion counts (all species-rank transitions, matching the template literally — no "actionable" filter) + Findings bullets that de-duplicate cross-host taxids, flag override policy gaps, flag bidirectional flips, and call out mechanical (genome-loss-driven) demotions. | Prose framing for each uncovered-by-existing-rules taxid: what changed upstream, ship-as-is or regen with new rules. |
| §5 Other notable changes | `index-params.json` change summary with high-signal callouts (kraken_db, viral_taxids_exclude_hard); virus taxonomy DB churn line. | Pipeline version range (from `output/logging/` in old/new indexes — see "Pipeline version" below); CHANGELOG narrative for substantive changes; `pipeline-min-index-version` coordination concerns. |
| Appendix A.1–A.15 | Lost gids by category (A.1–A.5); gained gids by category (A.6–A.10); full lost-species inventory (A.11); full gained-species inventory (A.12); per-host actionable transitions (A.13); params changes key-by-key (A.14); verbatim params diff (A.15). | Carry through to REVIEW.md verbatim or trim to the most relevant if length is a concern. |

**Categorization buckets** (what each label means — these are template categories with the script's specific semantics):

- **Lost gid categories** (priority order; each gid gets the first applicable bucket):
  - `hard_excluded`: gid's old species_taxid (or ancestor) is in `viral_taxids_exclude_hard` in the new build. The new exclude rule drops the gid at the filter stage.
  - `infection_status_change`: gid's old species lost ALL its gids in new metadata AND wasn't redistributed to a different taxid. The species's `infection_status` filter result flipped (could be `human`, `vertebrate`, or any host the filter checks — not just human), so the species drops out of the surveillance set.
  - `change_in_assigned_taxid`: gid's old species had its gids redistributed to a different species_taxid (NCBI/ICTV restructure); the gid was dropped under the new assignment.
  - `non_current_genome_version`: gid is gone but its species_taxid still has surviving gids in new metadata. Almost always the `assembly_status == 'current'` filter dropping a superseded NCBI assembly version.
  - `other`: no rule applies. Should be rare. If a meaningful shared-fate group ends up here (e.g. all SARS-CoV-2), surface it as a sub-bullet in §3.2 manually rather than leaving it generic.
- **Gained gid categories**:
  - `hard_included`: gid's new species_taxid (or ancestor) is in `ref/host-infection-overrides.json`. The include rule explains why this gid lands in the surveillance set.
  - `change_in_assigned_taxid`: gid's new species_taxid is the destination of an NCBI/ICTV restructure (some old species had its gids redistributed here). The taxid changed and the new assignment passes the filter.
  - `infection_status_change`: gid's new species was in the old taxonomy DB but NOT in old metadata (the species existed but failed the old filter); now it's in new metadata. The filter result flipped — the species is newly in the surveillance set.
  - `newly_deposited_existing`: gid's new species was already in old metadata (the species was already surveilled). New data for an already-tracked species.
  - `other`: should be rare. If a meaningful shared-fate group ends up here, surface it as a sub-bullet in §3.3.

### Step 3 — VHDB cross-reference (optional, only for the uncovered subset)

For any uncovered-by-existing-rules transition you're about to make a ship-or-regen call on (most often the 0→1 promotions or the override policy gaps), confirm the upstream direction by checking VHDB.

```bash
curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$6"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
```
Columns: virus tax id, virus name, disease, host tax id, host name.

For **1→0 human demotions**:
- Homo sapiens (9606) in column 4 → demotion is **not** upstream; investigate the workflow code (rare).
- No Homo sapiens but a human disease in column 3 → upstream VHDB drift; recommend regenerating with the taxid added to `ref/host-infection-overrides.json`.
- Host taxid `1` (`root`) in column 4 → VHDB has no specific host annotation; demotion is real. If the taxid is also in §3.2 / Appendix A.11 (lost-species list) with `redistributed_genome_count = 0` (i.e. its §A.13 demotion row has `Genome loss = yes`), the old `1` status was almost certainly ancestor-propagated through a higher-rank taxon that still has annotations; the demotion is mechanical and the right disposition is "ship as-is, no override".
- No row at all → try grepping by name. The actionable taxid is often a freshly-minted NCBI species ID; VHDB tends to lag by months and may still carry the host annotations under a legacy taxid.

For **0→1 promotions**:
- Homo sapiens now in column 4 → upstream VHDB addition. Then ask: is the species name structurally a recognised pathogen, or generic/placeholder (Bacteriophage sp., "Human gut <foo>", Microviridae, Smacoviridae, Picobirnaviridae)? If the latter → recommend regenerating with the taxid (or a broader family/class) added to `viral_taxids_exclude_hard`.
- No row at all → try grepping by name. The parent virus may carry the host annotations under a legacy taxid (e.g. *Orthobunyavirus turlockense* `3052452` returns nothing, but *Turlock virus* `35320` carries the host data). If the parent virus shows no Homo sapiens at any taxid, the promotion was carried by ancestor / descendant propagation rather than a direct VHDB Homo sapiens annotation — investigate before recommending an exclude.
- **Cross-reference §3.1 / Appendix A.8** for a paired `infection_status_change` row on the gain side. If the same taxid shows a large gid count in §3.1 (e.g. 1,349 *Bacteriophage sp.*), the trigger is a pipeline-driven flood of accessions plus VHDB drift. Cite the causal link explicitly.

**Bidirectional same-taxid flips.** When summary.md §4 surfaces a bidirectional flip (same taxid uncovered in both directions across different hosts), that's a fingerprint of upstream VHDB taxonomy churn. Investigate the upstream cause before recommending a `viral_taxids_exclude_hard` edit, since the latter would demote on every host including the legitimate ones.

**Pipeline version** for §5: read from `output/logging/` of each index — `pyproject.toml` (new builds) or `pipeline-version.txt` (older builds). The `pipeline-min-index-version` field surfaces the minimum RUN pipeline version that can consume the new index; coordinate downstream RUN deployments before promoting (this is a `coordination` recommendation, not a regen).

### Step 4 — Produce the report

Copy `review-template.md` to `<outdir>/REVIEW.md` and fill it in using data from `summary.md`. **Section structure, headings, and table shapes are non-negotiable — they come from the template.** Don't add extra columns to the §4 table; don't rename headers; don't reframe categories. Your job is to fill in:

- **Header**: target / reference index URIs and timestamp (copy from summary.md).
- **Summary**: Open with a one-line headline — **ready to release as-is** OR **needs regeneration with [X, Y]**. Follow with a tight bullet list (≤8 bullets) of top-level findings drawn from the per-section Findings. End with a `**Recommendations:**` sub-list that restates each Recommendation in one line. **The Summary must stand alone** — a colleague reading just the Summary should understand the ship-or-regen call.
- **Findings, §1–§5**: copy summary.md's tables and Findings bullets, then add prose interpretation where useful (one paragraph per section is usually enough). Don't tinker with the §4 table — its header is `| Host | Promotions | Demotions |` literally.
- **§3.2 / §3.3 discussion**: prose around the script-provided category breakdowns. Identify which losses / gains are surveillance-relevant; flag any species that warrant scientist review. If a meaningful shared-fate group sits in `other`, pull it out as its own bullet (don't leave generic "Other" bullets to hide real signal).
- **Recommendations**: ordered list of concrete ship-or-regen decisions. Each entry: `**Action summary** (confidence)` followed by a brief bulleted justification. Each action is one of:
  - "**Ship as-is**" — explicitly accept a finding.
  - "**Regenerate with [config change]**" — show the literal config diff that would fix the issue.
  - "**Coordination**" (for `pipeline-min-index-version` bumps or downstream consumer compatibility — these are about rolling out RUN, not changing the index itself).
  - Confidence levels: `high` (clear evidence), `scientist judgement` (low-confidence sanity check), `policy` (decision required, no obvious right answer), `coordination`.

  **Consolidate recommendations as one regen plan.** Regen cost is per-build (~2 hours), not per-fix. If ANY finding warrants regenerating, the marginal cost of folding in additional fixes is essentially zero — so any "ship as-is unless we're regenerating anyway" recommendations should flip to "regenerate with" once the overall decision is to regen. The headline should be a single ship-or-regen call with the full set of fixes the regen will include; the per-finding entries below it explain each fix's individual justification.

  **Never recommend "fix in the next build"**; if a fix is warranted, the action is regen now.
- **Appendix**: copy the appendix tables from `summary.md` (A.1 through A.15). Trim to the most relevant if length is a concern, but never replace with a pointer to another file — the report must stand alone.

**Trust the script's annotations.** If a row has `covered_by_hard_exclude = 2169574`, write "covered by Smacoviridae hard-exclude" — don't guess a family without checking. Don't manually classify coverage / family / reason — copy what the script said.

**De-duplicate cross-host findings**: if a single taxid is uncovered on multiple hosts, write it up once (under the highest-priority host it affected — human > primate > mammal > vertebrate > bird) and cross-reference from the others. The script already groups these in §4 Findings.

**Format translation note**: the `included_for_other_hosts` / "Override scope" column in `summary.md` is comma-separated (e.g. `human,vertebrate`); when writing a `ref/host-infection-overrides.json` diff, translate to a JSON array (`["human", "vertebrate"]`). Don't paste the column value verbatim into the JSON.

### Step 5 — Hand off

Print the `REVIEW.md` path back to the user with a 3-line inline summary (echoing the Summary headline). Don't open a PR or commit anything — recommendations need human judgment before going to code review.

Don't `rm -rf` the outdir between iterations; the script overwrites the files it owns, but `REVIEW.md` would go with the directory.

## What not to do

- **Don't invent your own report structure** — copy `review-template.md` verbatim and fill in the placeholders. The structure is the deliverable shape; deviating defeats the purpose of having a template.
- **Don't tinker with the §4 table header**. It's `| Host | Promotions | Demotions |` — three columns, no "actionable" qualifier, no extra dimensions. Counts are total species-rank transitions per host.
- **Don't recommend "fix in the next build"**. Every recommendation is a ship-or-regen decision about *this* index.
- **Don't link out to TSVs or to `summary.md`** in `REVIEW.md`. Pull whatever tables your reader needs into `REVIEW.md` directly (the Appendix is fine for long tables).
- **Don't itemise covered transitions or hard-excluded losses** beyond what the script's Findings bullets already say. They're absorbed by rules and need no action.
- **Don't leave a meaningful shared-fate group in `other`**. If 36 SARS-CoV-2 gids fall in `other`, surface them as a `**Non-current genome version (assembly_status filter): 36 gids**` sub-bullet or similar — be reluctant to use `other` as a hiding place.
- **Don't make up coverage or family claims.** If you'd otherwise write "covered by Smacoviridae", verify the script's annotation says so.
- **Don't conflate compressed file size with content size** for gzipped FASTAs/TSVs. Cite §2 Findings content-metric bullets when explaining a size delta.
- **Don't fabricate pathogen knowledge.** If you can't confidently identify whether a species is a human pathogen, say "flag for scientist review" rather than guess.
- **Don't act on recommendations.** Surface them; the user decides; config edits go through code review.

## Edge cases

- **No infection-status changes for a host**: leave the per-host row as `0 | 0` for completeness.
- **Old index missing a host group** (e.g., `infection_status_bird` only added later): note in §4 Findings that the host is newly tracked; no transition data to report.
- **`--repo-root` skipped**: the script falls back to no coverage / categorization annotation; you'll see "Coverage annotation unavailable" in summary.md §4 Findings. Re-run with `--repo-root .` from a mgs-workflow checkout.
- **A reference check errors out** (network blip): summary.md §1 shows `status: error` for that row. Note the inability to verify and continue.
- **Empty `genomes_added` / `genomes_removed`**: §3.1 categorized counts will all read 0; §3.2 / §3.3 collapse to "No genome IDs lost/gained". Note in Summary.
