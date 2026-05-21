---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so it can annotate transitions with existing rule coverage, classify lost / gained genome IDs by reason, and check reference-DB freshness), then maps the script's `summary.md` onto the structure in `review-template.md` to produce a written `REVIEW.md`. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

`bin/benchmark_index.py` does the heavy data work — it diffs sizes, content (FASTA records/bp, TSV row counts, metadata schema), infection-status transitions (annotated with `covered_by` / `included_for_other_hosts` / `driven_by_genome_loss` / `cross_host_actionable_on`), lost / gained genome IDs (each classified by reason: `hard_excluded` / `infection_status_demotion` / `species_retired` / `other` for losses; `hard_included` / `newly_deposited_existing` / `infection_status_promotion` / `species_new` / `other` for gains), lost-species inventories (annotated with `covered_by_hard_exclude`, `redistributed_to_species_taxid`, `redistributed_genome_count`, `truly_lost_count`), and reference-DB freshness (active checks for Kraken2 and SILVA). The script writes a self-contained `summary.md` structured around the same sections as `review-template.md`.

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

Takes ~60 seconds (most of it is staging the ~700 MB virus FASTAs for content-metric counts; the rest is small TSV downloads). If `<outdir>/summary.md` already exists, skip the script run and reuse — but note that to the user.

### Step 2 — Read `summary.md` end-to-end

`summary.md`'s sections map 1:1 onto `review-template.md`. Every table the template asks for is already populated; every `**Findings:**` block has script-generated factual bullets you can expand or quote directly. The reviewer's job in Step 4 is to interpret those Findings into prose, write a top-level Summary, decide §3.2 / §3.3 discussion priorities, and synthesize Recommendations.

Section map:

| `summary.md` section | What's pre-populated | What you add in REVIEW.md |
|---|---|---|
| §1 Staleness | Active check for Kraken2 + SILVA, passive URLs for human/taxonomy/VHDB; Findings bullets per stale ref. | Optional prose context (e.g. "DB is N months behind latest"). |
| §2 Database size | Compressed-bytes table + Findings bullets for FASTA records/bp deltas, TSV row counts, schema diff. | Prose interpretation when a compressed shrink hides content growth, or when a schema change drives bytes. |
| §3.1 Total | Categorized count bullets for lost + gained gids (see "Categorization buckets" below). | Nothing — paste verbatim. |
| §3.2 Losses | Top-5 species per loss category + summary of species-dropped-to-zero (with redistribution / true-loss split). | Prose discussion: which categories matter for surveillance, which are routine, any species that warrant scientist review. **When the `other` bucket includes routine surveillance pathogens (SARS-CoV-2, RSV, influenza, etc.), cross-reference `CHANGELOG.md` for assembly-status filter changes** — these losses are often the side-effect of an `assembly_status == 'current'` filter retroactively dropping superseded NCBI accessions, not an upstream data loss. |
| §3.3 Gains | Top-5 species per gain category. | Same shape as §3.2: prose discussion, surveillance-relevant callouts. |
| §4 Infection status | Per-host promotion / demotion counts (actionable only) + Findings bullets that de-duplicate cross-host taxids, flag override policy gaps, flag bidirectional flips, and call out mechanical (genome-loss-driven) demotions. | Prose framing for each actionable taxid: what changed upstream, why it's worth (or not worth) overriding. |
| §5 Other notable changes | `index-params.json` change summary with high-signal callouts (kraken_db, viral_taxids_exclude_hard); virus taxonomy DB churn line. | Pipeline version range (from `output/logging/` in old/new indexes — see "Pipeline version" below); CHANGELOG narrative for substantive changes; `pipeline-min-index-version` coordination concerns. |
| Appendix A.1–A.14 | Lost gids by category (A.1–A.4); gained gids by category (A.5–A.9); full lost-species inventory (A.10); full gained-species inventory (A.11); per-host actionable transitions (A.12); params changes key-by-key (A.13); verbatim params diff (A.14). | Carry through to REVIEW.md verbatim or by reference; trim to the most relevant appendices if length is a concern. |

**Categorization buckets** (what each label means — needed to interpret §3.1, §3.2, §3.3 correctly):

- **Lost gid categories** (priority order; each gid gets the first applicable bucket):
  - `hard_excluded`: gid's old species_taxid (or ancestor) is in `viral_taxids_exclude_hard` in the new build. The new exclude rule is the proximate cause.
  - `infection_status_demotion`: gid's species had `infection_status_human = 1` in old, `0` in new. Loss has surveillance impact (species was tracked, no longer is).
  - `species_retired`: gid's old species_taxid is gone from the new taxonomy DB. The species concept was retired.
  - `other`: no rule applies; a genuine upstream drop (NCBI suppressed the accession, dedup removed it, etc.).
- **Gained gid categories**:
  - `hard_included`: gid's new species_taxid (or ancestor) is in `ref/host-infection-overrides.json`. The include rule is why this gid lands in surveillance.
  - `newly_deposited_existing`: gid's new species already had `infection_status_human = 1` in old. Data growth for a known surveilled species.
  - `infection_status_promotion`: gid's new species had `0` in old, `1` in new. A previously-untracked species is now flagged human-infecting.
  - `species_new`: gid's species_taxid didn't exist in old taxonomy DB — brand-new species concept.
  - `other`: gid belongs to a known non-human-infecting species; reference-data growth without surveillance impact.

### Step 3 — VHDB cross-reference (optional, only for actionable items)

For any actionable transition you're about to recommend an edit for (most often the 0→1 human promotions or the override policy gaps), confirm the upstream direction by checking VHDB.

```bash
curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$6"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
```
Columns: virus tax id, virus name, disease, host tax id, host name.

For **1→0 human demotions**:
- Homo sapiens (9606) in column 4 → demotion is **not** upstream; investigate the workflow code (rare).
- No Homo sapiens but a human disease in column 3 → upstream VHDB drift; recommend adding to overrides.
- Host taxid `1` (`root`) in column 4 → VHDB has no specific host annotation; the demotion is real, don't recommend a `1`-override without external evidence. If the taxid is also in summary.md §3.2 / Appendix A.10 (true-loss list) — i.e. its §A.12 demotion row has `Genome loss = yes` — the old `1` status was almost certainly ancestor-propagated through a higher-rank taxon that still has annotations; the demotion is mechanical (no genomes → no propagation evidence) and the right disposition is "no action, no override".
- No row at all → try grepping by name. The actionable taxid is often a freshly-minted NCBI species ID; VHDB tends to lag by months and may still carry the host annotations under a legacy taxid.

For **0→1 promotions**:
- Homo sapiens now in column 4 → upstream VHDB addition. Then ask: is the species name structurally a recognised pathogen, or generic/placeholder (Bacteriophage sp., "Human gut <foo>", Microviridae, Smacoviridae, Picobirnaviridae)? If the latter → recommend adding the taxid (or a broader family/class) to `viral_taxids_exclude_hard`.
- No row at all → try grepping by name. The parent virus may carry the host annotations under a legacy taxid (e.g. *Orthobunyavirus turlockense* `3052452` returns nothing, but *Turlock virus* `35320` carries the host data). If the parent virus shows no Homo sapiens at any taxid, the promotion was carried by ancestor / descendant propagation rather than a direct VHDB Homo sapiens annotation — investigate before recommending an override.
- **Cross-reference §3.1 / Appendix A.7** for a paired `infection_status_promotion` row. If the same taxid shows a large gid count in §3.1 (e.g. 1,349 *Bacteriophage sp.*), the trigger is a pipeline parameter change pulling in many new accessions, not just VHDB drift. Cite the causal link explicitly.

**Bidirectional same-taxid flips.** When summary.md §4 surfaces a bidirectional flip (same taxid actionable in both directions across different hosts), that's a fingerprint of upstream VHDB taxonomy churn — the species got new host annotations on some columns and lost them on others. Treat as a distinct narrative pattern: investigate the upstream cause before recommending a `viral_taxids_exclude_hard` edit, since the latter would demote on every host including the legitimate ones.

**Pipeline version** for §5: read from `output/logging/` of each index — `pyproject.toml` (new builds) or `pipeline-version.txt` (older builds). The `pipeline-min-index-version` field surfaces the minimum RUN pipeline version that can consume the new index; coordinate downstream RUN deployments before promoting.

### Step 4 — Produce the report

Copy `review-template.md` to `<outdir>/REVIEW.md` and fill it in using data from `summary.md`. Section structure, headings, and table shapes are non-negotiable — they come from the template. Your job is to fill in:

- **Header**: target / reference index URIs and timestamp (copy from summary.md).
- **Summary**: bullet list of top-level findings, drawing on the per-section Findings. End with a `**Recommendations:**` sub-list that restates the concrete actions from the Recommendations section in one line each. Stand-alone — the Summary must make sense without reading the rest.
- **Findings, §1–§5**: copy summary.md's tables and Findings bullets, then add prose interpretation where useful (one paragraph per section is usually enough).
- **§3.2 / §3.3 discussion**: prose around the script-provided category breakdowns. Identify which losses / gains are surveillance-relevant; flag any species that warrant scientist review.
- **Recommendations**: ordered list of concrete actions. Each entry: `**Action summary** (confidence)` followed by a brief bulleted justification. Confidence levels: `high` (clear evidence), `scientist judgement` (low-confidence sanity check), `policy` (decision required, no obvious right answer), `next-build hygiene` (e.g. staleness bump), `coordination` (downstream consumer impact).
- **Appendix**: copy the appendix tables from `summary.md` (A.1 through A.14). Trim to the most relevant if length is a concern, but never replace with a pointer to another file — the report must stand alone.

**Trust the script's annotations.** If a row has `covered_by_hard_exclude = 2169574`, write "covered by Smacoviridae hard-exclude" — don't guess a family without checking. If a gid is categorized `infection_status_promotion`, the script has verified the species went 0→1 in `infection_status_human`. Don't manually classify coverage / family / reason — copy what the script said.

**De-duplicate cross-host findings**: if a single taxid is actionable on multiple hosts, write it up once (under the highest-priority host it affected — human > primate > mammal > vertebrate > bird) and cross-reference from the others. The script already groups these in §4 Findings.

**Format translation note**: the `included_for_other_hosts` / "Override scope" column in `summary.md` is comma-separated (e.g. `human,vertebrate`); when writing a `ref/host-infection-overrides.json` diff, translate to a JSON array (`["human", "vertebrate"]`). Don't paste the column value verbatim into the JSON.

### Step 5 — Hand off

Print the `REVIEW.md` path back to the user with a 3-line inline summary. Don't open a PR or commit anything — recommendations need human judgment before going to code review.

Don't `rm -rf` the outdir between iterations; the script overwrites the files it owns, but `REVIEW.md` would go with the directory.

## What not to do

- **Don't invent your own report structure** — copy `review-template.md` verbatim and fill in the placeholders. The structure is the deliverable shape; deviating defeats the purpose of having a template.
- **Don't link out to TSVs or to `summary.md`** in `REVIEW.md`. The TSVs are the script's working data; `summary.md` is its self-contained data file; `REVIEW.md` is the agent's interpretation. Each is self-contained. Pull whatever tables your reader needs into `REVIEW.md` directly (the Appendix is fine for long tables).
- **Don't itemise covered transitions or hard-excluded losses** beyond what the script's Findings bullets already say. They're absorbed by rules and need no action.
- **Don't make up coverage or family claims.** If you'd otherwise write "covered by Smacoviridae", verify the script's annotation says so. If it doesn't, don't.
- **Don't conflate compressed file size with content size** for gzipped FASTAs/TSVs. Cite §2 Findings content-metric bullets when explaining a size delta.
- **Don't fabricate pathogen knowledge.** If you can't confidently identify whether an actionable species is a human pathogen, say "flag for scientist review" rather than guess.
- **Don't act on recommendations.** Surface them; the user decides; config edits go through code review.

## Edge cases

- **No infection-status changes for a host**: skip the per-host row in §4 if the count is 0/0 or just leave it as `0 | 0` for completeness.
- **Old index missing a host group** (e.g., `infection_status_bird` only added later): note in §4 Findings that the host is newly tracked; no transition data to report.
- **`--repo-root` skipped**: the script falls back to no coverage / categorization annotation; you'll see "Coverage annotation unavailable" in summary.md §4 Findings. Re-run with `--repo-root .` from a mgs-workflow checkout.
- **A reference check errors out** (network blip): summary.md §1 shows `status: error` for that row. Note the inability to verify and continue.
- **Empty `genomes_added` / `genomes_removed`**: §3.1 categorized counts will all read 0; §3.2 / §3.3 collapse to "No genome IDs lost/gained". Note in Summary.
