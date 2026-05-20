---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so it can annotate transitions with existing rule coverage and classify lost genomes by redistribution / hard-exclude), then turns the script's `summary.md` into a written review with a headline recommendation and concrete config edits. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

`bin/benchmark_index.py` does the heavy data work — it diffs sizes, content (FASTA records/bp, TSV row counts, metadata schema), infection-status transitions (annotated with `covered_by` / `included_for_other_hosts`), lost-genome inventories (annotated with `covered_by_hard_exclude`, `redistributed_to_species_taxid`, `redistributed_genome_count`, `truly_lost_count`), and reference-DB freshness (active checks for Kraken2 and SILVA). The script writes a self-contained `summary.md`.

Your job is to read `summary.md` and turn its structured data into a written `REVIEW.md` aimed at a colleague who hasn't seen this conversation — adding a one-paragraph headline, a "how to read this report" preamble, and concrete config-edit recommendations. The narrative is yours; the arithmetic is the script's.

## When to use

- The user wants to vet a new index release before promoting it to production.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no written review), just run the script and surface `summary.md` — don't write `REVIEW.md`.

## Inputs

- `--old <root>`: parent of `output/` for the old index. `s3://...` or local path. **Required.**
- `--new <root>`: parent of `output/` for the new index. **Required.**
- `--out <dir>`: output directory. Use an absolute path so paths in the report are reader-portable.
- `--repo-root <path>`: a mgs-workflow checkout. Without this, the script falls back to plain counts; with it you get all the coverage / redistribution / hard-exclude annotations that drive the report.

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

The script's output is structured to feed the report directly. Each `summary.md` section maps to a `REVIEW.md` section:

| `summary.md` section | What it gives you |
|---|---|
| §1 Reference-DB staleness | Auto-active check for Kraken2 + SILVA; passive URL display for human / NCBI taxonomy / VHDB. Cite any **stale** flag in the headline. **Staleness applies to the URL pinned for the *next* build, not this one** — never a blocker for promoting an index that's already built. Frame as "bump for the next build" in the recommendations, not as a regression. |
| §2 Per-DB sizes + §2.1 content metrics + §2.2 schema diff | Tells the real story about size deltas. Compressed bytes are misleading for gzipped FASTAs/TSVs; surface the content-metrics columns (records, total_bp, rows) for that. Schema diff explains most of any metadata-file shrink. |
| §3 Virus genomes (3.1 true losses / 3.2 redistributed / 3.3 covered) | The script has already split by redistribution + hard-exclude coverage. Use §3.1 (true losses) for your "concerning" list; §3.2 (redistributed) is informational; §3.3 is dispatched in one sentence. |
| §5 Infection-status changes | Per-host actionable table + drill-down with every actionable row inline. Pull these directly into your report; don't paraphrase, don't repeat covered rows. |
| Appendix A (covered transitions) | Only consult if you want to spot-check a specific covered claim — usually skip. |
| Appendix B (full lost-genomes) | Reference if a user asks for the full inventory. |
| Appendix C (verbatim params diff) | The basis for your §4 (Other notable changes). Read `CHANGELOG.md` in the workflow checkout for the human "why" behind each version-bump-driven change. |

**Trust the script's annotations.** If a row has `covered_by_hard_exclude = 2169574`, write "covered by Smacoviridae hard-exclude" — don't guess a family without checking. If a row has `redistributed_genome_count = N`, the genomes went *somewhere* in the new metadata; report N / old_count and the destination taxid + name.

### Step 3 — VHDB cross-reference (optional, only for actionable items)

For any actionable transition you're about to recommend an edit for (most often the 0→1 human promotions), confirm the upstream direction by checking VHDB.

```bash
curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$6"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
```
Columns: virus tax id, virus name, disease, host tax id, host name.

For **1→0 human demotions**:
- Homo sapiens (9606) in column 4 → demotion is **not** upstream; investigate the workflow code (rare).
- No Homo sapiens but a human disease in column 3 → upstream VHDB drift; recommend adding to overrides.
- Host taxid `1` (`root`) in column 4 → VHDB has no specific host annotation; the demotion is real, don't recommend a `1`-override without external evidence. If the taxid is also in summary.md §3.1 (true-loss table) — i.e. the §5.x demotion row has `Genome loss = yes` — the old `1` status was almost certainly ancestor-propagated through a higher-rank taxon that still has annotations; the demotion is mechanical (no genomes → no propagation evidence) and the right disposition is "no action, no override".
- No row at all → species may have been renamed and VHDB still has the old taxid; try grepping by name. If still nothing, don't recommend an override from the benchmark alone.

For **0→1 promotions**:
- Homo sapiens now in column 4 → upstream VHDB addition. Then ask: is the species name structurally a recognised pathogen, or generic/placeholder (Bacteriophage sp., "Human gut <foo>", Microviridae, Smacoviridae, Picobirnaviridae)? If the latter → recommend adding the taxid (or a broader family/class) to `viral_taxids_exclude_hard`.
- No row at all → try grepping by name. The actionable taxid is often a freshly-minted NCBI species ID; VHDB tends to lag by months and may still carry the host annotations under a legacy taxid (e.g. *Orthobunyavirus turlockense* `3052452` returns nothing, but *Turlock virus* `35320` carries the host data). If the parent virus shows no Homo sapiens at any taxid, the promotion was carried by ancestor / descendant propagation rather than a direct VHDB Homo sapiens annotation — investigate before recommending an override.
- **Cross-reference `genomes_by_species.tsv`** (in the output dir) for the same taxid. If the promotion is paired with a large `delta` (e.g. 0 → 1349), the trigger is likely a pipeline parameter change pulling in many new accessions, not VHDB drift. Cite the causal link explicitly.

**Bidirectional same-taxid flips.** When the same `taxid` appears in both the `1→0` and `0→1` columns across different hosts (look for it in summary.md's per-host §5.x tables), that's a fingerprint of upstream VHDB taxonomy churn at the species rank — the species got new host annotations on some columns while losing them on others, typically because VHDB moved an entry between related taxids. Treat as a distinct narrative pattern from a single-direction call: investigate the upstream cause before recommending an edit, since a `viral_taxids_exclude_hard` entry will demote on *every* host including the legitimate ones.

### Step 4 — Produce the report

Write `<outdir>/REVIEW.md`. Overwrite if it exists.

**The report must be readable in isolation.** A colleague who hasn't seen this conversation, doesn't know the workflow's history, and isn't familiar with the override/exclude mechanisms must be able to read REVIEW.md cold and understand what changed and what to do about it. Concretely:

- Always include the "How to read this report" preamble below (4 items).
- Don't refer the reader to TSVs or to other files in the output directory. Embed every table you need inline — long tables go in an appendix at the end of REVIEW.md, not as a pointer to elsewhere.
- Use **absolute paths** when you must reference output-dir files (e.g. for the user to look at the underlying data themselves).
- Don't reference PRs by number without explaining what they did.
- Don't say "the usual pattern" or "we already understood" — say what the pattern *is*.
- Trust the script's coverage / redistribution annotations; don't manually classify coverage or guess at families.
- De-duplicate cross-host findings: if a single taxid is actionable on multiple hosts, write it up once (under the highest-priority host it affected — human > primate > mammal > vertebrate > bird) and cross-reference from the others.
- **Format translation note**: the `included_for_other_hosts` / "Override scope" column in summary.md is comma-separated (e.g. `human,vertebrate`); when writing a `ref/host-infection-overrides.json` diff, translate to a JSON array (`["human", "vertebrate"]`). Don't paste the column value verbatim into the JSON.

Five sections, in order. Lead with a one-paragraph headline that stands on its own.

````markdown
# Index benchmark review: <OLD> → <NEW>

**Headline**: <one paragraph readable in isolation. Concrete shape:
"Ready to promote / Not ready to promote, because <reason>. <count>
actionable item(s) — <one-line each>. <reference-DB staleness call if
any>. <pointer to any policy question>." The example shape here is
intentionally generic; don't copy it.>

---

## How to read this report

- **`infection_status_<host>` columns**: every viral taxon carries five
  columns (`human`, `primate`, `mammal`, `vertebrate`, `bird`). `1` =
  infects, `0` = does not, `2` = unknown, `3` = likely. Values come from
  upstream Virus-Host-DB (VHDB).
- **Two workflow override mechanisms** correct VHDB mis-annotations:
  - `viral_taxids_exclude_hard` (in `configs/index.config`): taxids
    forced to `0` for every host, applied to all descendants. Used for
    whole families of known false positives.
  - `ref/host-infection-overrides.json`: `{taxid, hosts}` entries
    forcing `1` for the listed hosts. Used for known human pathogens
    VHDB mis-classifies.
- **"Covered" vs "actionable" transitions**: when a status flips between
  the two indexes, the benchmark script checks whether one of the rules
  above already explains it. Covered → no action; actionable → needs
  human review.
- **"Override policy gap"**: actionable demotion whose `species_taxid` IS
  in `ref/host-infection-overrides.json` but only for *other* hosts.
  Either widen the override's `hosts` list or accept the drift.

Underlying data: `<absolute outdir path>/`. The script's `summary.md`
contains every table this report is built from, plus an appendix with
the full lost-genomes inventory and verbatim params diff.

---

## 1. Reference-DB staleness

[Table from summary.md §1. Bold any "stale" flag. One sentence on what
to do (e.g., "Kraken DB is 7 months out of date; consider bumping
before the next index build." or "All references are current.")]

## 2. Per-DB sizes

[Table from summary.md §2 (compressed bytes), plus a brief callout for
each shrunk DB. Always pair "compressed shrank" with the content metric
from §2.1 — if records/total_bp grew, the compressed shrink is a gzip
artifact, not a content regression. Surface the schema diff from §2.2
when relevant: "Most of the metadata-file shrink is the 18-column schema
reduction, not row loss."]

## 3. Virus genomes

[Top-line counts. Three sub-sections matching the script's split:
- 3.1 True losses (table from summary.md §3.1, dispatched per row with
  one-line context per non-trivial entry).
- 3.2 Redistribution (one paragraph + table). Call out any notable
  destinations (e.g. "Human adenovirus 89 → Simian adenovirus 45 is a
  documented NCBI reassignment; 310 genomes moved").
- 3.3 Covered by hard-exclude (one sentence: "N species in this bucket
  are absorbed by the existing Smacoviridae / Picobirnaviridae / ...
  excludes and have no surveillance impact.").]

## 4. Infection-status changes

[Per-host table from summary.md §5. For each host with actionable rows,
write a short prose paragraph naming the taxa (using the §5.x inline
tables). De-duplicate cross-host findings: write up each taxid once.
Address any override policy gap explicitly.]

## 5. Other notable changes

[Pipeline version range (from `pyproject.toml` and `CHANGELOG.md`).
Mine the CHANGELOG for the "why" behind each substantive change. Surface
any `pipeline-min-index-version` bump that downstream RUN consumers
need to coordinate with.]

## 6. Recommendations

[Concrete config edits, ordered by confidence (highest first):

1. High-confidence edits with clear evidence (VHDB false positive,
   pipeline-driven contamination). Show the literal before/after diff:

   ```
   - viral_taxids_exclude_hard = "..."
   + viral_taxids_exclude_hard = "... <new-taxid>"
   ```

2. Scientist-judgment items (low-confidence but worth a sanity check).

3. Policy questions (e.g. override-scope), stated as questions, not
   unilateral recommendations.

4. Reference-DB bumps if §1 flagged anything stale.

If no config changes are needed, say so explicitly.]

## Appendix — Full <whatever you need>

[If a reader might want the full lost-genomes inventory or the full
covered-transitions list, embed it here as a markdown table. Pull from
summary.md's appendices.]
````

### Step 5 — Hand off

Print the `REVIEW.md` path back to the user with a 3-line inline summary. Don't open a PR or commit anything — recommendations need human judgment before going to code review.

Don't `rm -rf` the outdir between iterations; the script overwrites the files it owns, but your `REVIEW.md` would go with the directory.

## What not to do

- **Don't itemise covered transitions.** The script already pre-filtered them; only name the *actionable* ones. Aggregate the covered count in one line per host if useful.
- **Don't make up coverage claims.** When the report says "covered by Smacoviridae", that has to come from the script's `covered_by_hard_exclude = 2169574` column, not a guess at family membership.
- **Don't conflate compressed file size with content size** for gzipped FASTAs/TSVs. Cite §2.1 content metrics when explaining a size delta.
- **Don't refer the reader to TSVs or to summary.md.** The TSVs are the script's working data; `summary.md` is its self-contained report; `REVIEW.md` is yours, also self-contained. Pull whatever tables your reader needs into `REVIEW.md` directly.
- **Don't fabricate pathogen knowledge.** If you can't confidently identify whether an actionable species is a human pathogen, say "flag for scientist review" rather than guess.
- **Don't act on recommendations.** Surface them; the user decides; config edits go through code review.

## Edge cases

- **No infection-status changes for a host**: skip the §4 bullet for that host rather than writing "no changes".
- **Old index missing a host group** (e.g., `infection_status_bird` only added later): note in §4 that the host is newly tracked; no transition data to report.
- **`--repo-root` skipped**: the script falls back to no coverage / redistribution annotation; you'll see "(coverage unavailable)" in summary.md. Re-run with `--repo-root .` from a mgs-workflow checkout.
- **A reference check errors out** (network blip): summary.md §1 will show `status: error` for that row. Note the inability to verify and continue.
- **Pipeline-min-index-version bump**: if `CHANGELOG.md` mentions one in the version range, surface it in §5 — RUN deployments need to be on a compatible version before consuming the new index.
