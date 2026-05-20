---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so per-species infection-status transitions are annotated with whether existing `ref/host-infection-overrides.json` or `viral_taxids_exclude_hard` rules already cover them), then interprets the *uncovered* (actionable) subset — drills into the human-infection list first, cross-references upstream Virus-Host-DB only for genuinely uncovered demotions to attribute the change to upstream drift vs a workflow change, spot-checks species that lost all their genomes, and proposes concrete config edits (additions to `ref/host-infection-overrides.json` or `viral_taxids_exclude_hard`) keyed to the findings. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

Compare an `--old` and `--new` index release, produce a structured markdown review that a maintainer can paste into a Slack thread / PR description / Linear ticket. The script (`bin/benchmark_index.py`) does the deterministic data extraction *and* the cross-reference against existing exclude/override rules; this skill turns the pre-filtered output into a written review with concrete recommendations.

## When to use

- The user wants to vet a new index release before promoting it to production.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no interpretation), just run the script — don't write the review.

## Inputs

- `--old <root>`: parent of `output/` for the old index. `s3://...` or local path. **Required.**
- `--new <root>`: parent of `output/` for the new index. **Required.**
- `--out <dir>` (optional): output directory. Default `./bench-<old-tag>-vs-<new-tag>/` derived from the URI basenames.

If either root is missing, ask the user — don't guess.

## Procedure

### Step 1 — Run the script

Always invoke with `--repo-root .` (assuming you're at a mgs-workflow checkout root) so per-species transitions get annotated with `covered_by` ("excluded" | "included" | ""). Without that flag, you'd have to do the cross-reference manually:

```bash
python bin/benchmark_index.py --old <old> --new <new> --out <outdir> --repo-root .
```

If `<outdir>` already has `summary.md` and the per-host `species_transitions_*.tsv` files, skip and reuse — flag to the user that you reused. Re-running is cheap (~10MB of small file downloads).

Read `<outdir>/summary.md` first. It surfaces the actionable counts directly:
- Per-host: total transitions / **uncovered** 1→0 demotions / **uncovered** 0→1 promotions
- Top-10 species that lost all genomes
- Shrunk DBs

The interpretation work below is about turning those counts into a written report.

### Step 2 — Drill into the uncovered subset

The script has already filtered to the actionable rows. Your job is to name them and decide what to recommend.

**`species_transitions_human.tsv`** is the most important file. Filter the rows you read to `covered_by == ""` — those are the genuine concerns. The rest are explained by existing rules in `ref/host-infection-overrides.json` or `viral_taxids_exclude_hard` and don't need action.

For each **uncovered 1→0 human demotion**:
- Look up the species by name. Is it a known human pathogen?
- If yes → recommend adding it to `ref/host-infection-overrides.json` (see Step 3 for the VHDB confirmation step).
- If unsure → flag for scientist review rather than recommending an override.

For each **uncovered 0→1 human promotion**:
- Pattern-match the name. Bacteriophage / Microviridae / gokushovirus / Smacoviridae / Picobirnaviridae / "Human gut <foo>" → false positive, recommend adding the species (or, better, its family/class) to `viral_taxids_exclude_hard`.
- Anything else → flag for scientist review.

For the other hosts (`bird`, `mammal`, `primate`, `vertebrate`), the bar is lower — only flag if uncovered counts are unusually high or include conspicuous names. Most non-human animal-pathogen reannotations don't affect the human-surveillance use case.

**Watch the `included_for_other_hosts` column** in each `species_transitions_<host>.tsv`. When this column is non-empty for an uncovered demotion, it means the taxid IS in `ref/host-infection-overrides.json` — just for different host(s) than the one demoting. summary.md surfaces the count as "N policy gap(s)" inline. This is a scope-of-override question: do we want the override to apply across the whole human-bearing taxonomic chain (human / primate / mammal / vertebrate), or only the hosts the entry explicitly lists? Flag the gap in the report but don't unilaterally recommend expanding scope — it's a policy call.

**Heuristic for "bar for flagging" on non-human hosts**: by default, only single out by name uncovered transitions for `bird` / `mammal` / `primate` / `vertebrate` if either (a) the actionable count is materially larger than for `human` (rule of thumb: ≥5× the human count for the same direction), or (b) the names include a recognizable human/livestock pathogen (mention SARS-CoV-2 / influenza / hantavirus / orthopox / rabies / ebola / hepatitis / etc. → name it; if it's clearly an environmental / arthropod / non-mammalian-host virus → aggregate). When uncertain about a name, say "flag for scientist review" rather than recommending action — these columns matter less for human-surveillance reporting and the cost of a missed call is low.

**`species_lost_all_genomes.tsv`** — species with `new_count=0` and `old_count>0`. For each in the top 10–20:
- Is it a known human pathogen? If yes → real concern; recommend investigating whether a `download_virus_taxid` config change is needed.
- Otherwise (smacovirus, environmental virus, etc.) → likely fine.
- **Caveat**: NCBI taxonomy renames cause false positives here. If a species name "disappears" but the new taxonomy DB has a similar name under a *different* taxid (i.e. the species was rekeyed), the genomes likely moved with it. Spot-check by searching the new taxonomy:
  ```bash
  zcat <outdir>/../*-db/total-virus-db-annotated.tsv.gz 2>/dev/null | grep -i "<species name>" | head
  ```
  Or do a quick web search on the species name to see if NCBI's taxonomy browser shows a rename.

**`sizes.tsv`** — any negative `delta_bytes`? Common explanations to mention:
- `virus-genomes-masked.fasta.gz` + `virus-genome-metadata-gid.tsv.gz` both shrinking → assembly-status filter (current-only) is dropping superseded NCBI assemblies. Normal since the v3.2.1.5 rework; not a regression.
- A bowtie2 / minimap2 index identical to old → reference URL unchanged.

**`params_diff.txt`** — look for:
- `kraken_db` URL change → note the date version.
- Pipeline version bump (top of `pyproject.toml`, if you can see it in the diff) → mention the release range.
- New / removed `params.*` keys → list them.
- Path-prefix-only changes (`/repo/ref/` → `/ref/`) → cosmetic, ignore.

**`taxa_added.tsv` / `taxa_removed.tsv`** — usually thousands of rows from NCBI taxonomy churn. Don't itemise; just note the magnitude in the report.

### Step 3 — Confirm uncovered demotions against upstream VHDB

Only needed if Step 2 produced **uncovered 1→0 human demotions** worth following up on. Skip otherwise.

Fetch the current VHDB once:
```bash
curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
```

For each uncovered demoted `<taxid>`:
```bash
awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$6"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
```
- Homo sapiens (9606) in column 8 → the demotion is **not** upstream; investigate the workflow code (rare).
- No Homo sapiens but a human disease named in column 6/7 → demotion **is** upstream VHDB drift (the host annotation isn't capturing what the disease annotation says). Typical pattern; recommend adding to overrides.
- No Homo sapiens, no human disease → either the demotion is correct, or VHDB has no human data at all. Don't recommend an override without a separate evidence source (textbook, recent literature).

### Step 4 — Produce the report

Write a markdown report to `<outdir>/REVIEW.md` (overwrite if it already exists — the script doesn't manage this file, so a stale copy from a previous skill run won't be touched without you doing it). Surface the report inline in the user-facing reply.

**The report must be readable in isolation.** A colleague who hasn't seen this conversation, doesn't know the workflow's history, and isn't familiar with the override/exclude mechanisms should be able to read REVIEW.md cold and understand (a) what changed between indexes, (b) what to do about it. Concretely:

- **Always include a "How to read this report" preamble** between the headline and Section 1, defining the three pieces of background a reader needs: the `infection_status_<host>` columns and what their values mean, the two override mechanisms (`viral_taxids_exclude_hard` and `ref/host-infection-overrides.json`), and the "covered" / "actionable" distinction the per-host table uses.
- Don't reference PRs by number without explaining what they did.
- Don't say things like "we already understood" or "the usual pattern" — say what the pattern is.
- Prefer **tables** for per-DB sizes and per-host transition counts (much easier to scan than prose). Bold counts that require action.
- Use the word **actionable** (or "needs review") rather than **uncovered** in prose, with one inline definition near first use. "Uncovered" reads like jargon to a fresh reader.

Five sections, in order. **Lead with a one-paragraph headline**: is the index ready to promote, with what (if any) changes required? Make the headline meaningful when read alone.

```markdown
# Index benchmark review: <OLD> → <NEW>

**Headline**: <one-paragraph summary that can be quoted on its own.
"Ready to promote with one config tweak — add `38018` (Bacteriophage sp.)
to the hard-exclude list. Everything else is either already covered by
existing rules, a taxonomy rename rather than a real loss, or non-human
drift with no surveillance impact." or similar.>

---

## How to read this report

- **`infection_status_<host>` columns**: every viral taxon in the index
  carries five columns (`human`, `primate`, `mammal`, `vertebrate`, `bird`)
  saying whether the species infects that host. `1` = infects, `0` = does
  not, `2` = unknown, `3` = likely. Values come from upstream Virus-Host-DB
  (VHDB) and are recomputed each index build, so they drift between
  releases as VHDB updates.
- **Two workflow override mechanisms** correct known VHDB mis-annotations:
  - `viral_taxids_exclude_hard` (in `configs/index.config`): a space-
    separated list of taxids forced to status `0` for every host, along
    with all their descendants. Used for whole families of known false
    positives.
  - `ref/host-infection-overrides.json`: `{taxid, hosts}` entries that
    force status `1` for the listed hosts. Used for known human pathogens
    that VHDB mis-classifies.
- **"Covered" vs "actionable" transitions**: when a status flips between
  the two indexes, the benchmark script checks whether one of the two
  mechanisms above already explains the flip. If yes → covered (no action
  needed; the workflow intentionally overrides VHDB). If no → actionable
  (genuine, unmediated VHDB drift that a human should look at). Counts
  in **bold** in the table below are actionable rows.

---

## 1. Per-DB sizes

[Table with columns: DB, Old, New, Δ (absolute + percent), Notes.
Use human-readable units (GB / MB / KB). Bold the Δ for any DB that
shrunk. Inline one-line note per DB if it shrank (likely cause) or
is new. Group byte-identical DBs into a single row to save space.]

## 2. Infection-status changes

[Open with the per-host table (Host, Total transitions, Actionable 1→0,
Actionable 0→1, Policy gaps — all numeric). Bold the actionable
counts. Then drill into each host with a non-zero actionable count by
name; aggregate the covered counts in one line per host. Bigger bars
for non-human hosts (per Step 2 heuristic).]

## 3. Lost virus genomes

[Top-line: N added, M removed, K species went to zero. The K count
should already be split into "likely renames" vs "true losses" by the
script (see `species_lost_all_genomes.tsv`'s `likely_rename` column).
Table the top true losses with the likely-rename column visible.
Don't itemise the renames unless one is conspicuous.]

## 4. Other notable changes

[Pipeline version bump (call out the range explicitly), new/removed
params, schema changes in any of the data files. Cosmetic path
changes can be mentioned and dismissed in one sentence.]

## 5. Recommendations

[Concrete config edits, keyed to findings:
- "Add `<taxid>` (`<name>`) to `viral_taxids_exclude_hard` in
  `configs/index.config`, `configs/index-for-run-test.config`, and
  `tests/configs/index.config`: <reason>". Show the literal before/after
  for the edit when small.
- "Add `<taxid>` (`<name>`) to `ref/host-infection-overrides.json`:
  <reason>".
- Policy questions (e.g. override scope) stated as a question, not a
  unilateral recommendation.
If no config changes are needed, say so explicitly in section 5.]
```

### Step 5 — Hand off

Write `REVIEW.md` to `<outdir>/REVIEW.md` and print its path back to the user with a 3-line summary inline. The script does **not** clobber `REVIEW.md` on re-run (it only writes files it owns), but if you `rm -rf` the outdir between iterations of the skill — to force a fresh script run — your `REVIEW.md` goes with it. Don't `rm -rf` the outdir; let the script overwrite the TSVs in place.

Don't open a PR or commit anything — recommendations need human judgment before they're committed.

## What not to do

- **Don't itemise covered transitions.** The script already pre-filtered them out. Only name the *uncovered* ones. (Aggregate covered counts in one line per host if useful.)
- **Don't fabricate pathogen knowledge.** If you can't confidently identify whether an uncovered species is a human pathogen, say "flag for scientist review" rather than guess.
- **Don't paste raw TSVs.** The user can `less` them; the report's job is to name the specific findings and recommend actions.
- **Don't act on recommendations.** Surface them; let the user decide. Config edits go through code review.
- **Don't loop the VHDB cross-reference.** Fetch the file once at the top of Step 3 and grep against the local copy.

## Edge cases

- **No infection-status changes for a host**: skip the bullet for that host rather than writing "no changes."
- **Schema change in `virus-genome-metadata-gid.tsv.gz`** (column set differs): the script's `diff_genome_metadata` already restricts to common columns; mention the schema difference in Section 4.
- **One-off staging failures** (network blip on `aws s3 cp`): retry the script before falling back to a partial report.
- **Old index missing a host group** (e.g., `infection_status_bird` only added later): note in Section 2 that the host is newly tracked; no transition data to report.
- **`--repo-root` skipped or pointed at the wrong place**: the script falls back to no coverage annotation and summary.md shows "X species 1→0" instead of "X uncovered 1→0". If you see that, re-run with `--repo-root .` from a mgs-workflow checkout.
- **Taxonomy rename masquerading as genome loss**: see Step 2 caveat under `species_lost_all_genomes.tsv`. Always sanity-check the top entries.
