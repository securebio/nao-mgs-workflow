---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured review report (`REVIEW.md`). Use to vet a newly built dated `s3://nao-mgs-index/` index before adopting it for runs.
---

# Benchmark an index release

The report helps the reviewer decide whether to ship this index as-is or change
something first. `bin/benchmark_index.py` does the deterministic data extraction;
you fill in `review-template.md` from its summary JSON and TSV outputs.

**`review-template.md` is the source of truth for report structure** — open it
and fill it in literally; the skill only adds procedural support and a category
glossary. If the template and skill disagree, the template wins.

`REVIEW.md` must stand alone: embed the needed tables and facts rather than
pointing at output files ("see genomes_summary.json"). Appendix tables can be
pasted naively from TSVs — self-containment matters more than polish.

## When to use

- The user wants to vet a newly built index before adopting it for runs.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no written review), run the script
and surface the output directory plus the relevant summary files; do not write
`REVIEW.md`.

## Inputs

- `new_index` (required): the new (candidate) index to vet — an `s3://nao-mgs-index/<DATE>` URI or a local path (the root containing `output/`).
- `old_index` (required): the old (reference) index to compare against, same form.
- `out_dir` (required): directory for the report and tables; use an absolute path.

If any is missing, ask the user; do not guess. These map to the script's `--new`,
`--old`, and `--out` (Step 1). Coverage annotations are derived from `new_index`
itself, so no repo checkout is needed.

Both indexes must publish `output/results/virus-genomes-masked.fasta.gz`, which
the script reads to compare the two genome DBs by membership; `new_index` must
additionally publish `virus-genome-metadata-raw.tsv.gz` and
`input/host-infection-overrides.json`. An index whose objects have been
transitioned to Glacier cannot be staged at all and so cannot be used on either
side — the resulting `Could not stage ...` abort is that, not a script defect.
Restoring the objects, or picking a still-warm reference index, is the fix.

## Procedure

### Step 1 - Run the script

```bash
python bin/benchmark_index.py \
  --old <reference-index-root> \
  --new <candidate-index-root> \
  --out <output-dir>
```

Use **absolute** paths for `--out` (e.g. `/tmp/bench-...`). Takes a couple of
minutes, most of it spent staging each index's genome DB FASTA — the largest
file in an index, and read twice per side: once for its content metrics and
once for its sequence IDs. If `<outdir>` already contains benchmark outputs,
reuse them only if the user asked you to avoid rerunning; otherwise rerun so
the reference freshness checks are current.

### Step 2 - Read summaries and TSVs

Read the compact script-produced summaries before interpreting detail rows:

- `sizes_summary.json`: counts of top-level output entries that grew, shrank,
  or stayed unchanged.
- `genomes_summary.json`: headline genome/taxonomy counts — lost/gained totals,
  per-reason counts, all-lost / all-gained species, reassignments, net delta,
  taxa added/removed, and the four metadata/genome-DB agreement counts read in
  Step 2a. If `lost_total` or `gained_total` is zero, §3.2 / §3.3 collapse to
  "No genome IDs lost/gained".
- `index_versions.json`: the pipeline version each index was built with
  (`pipeline_version_old`, `pipeline_version_new`), read from the index's own
  published `pyproject.toml`, or from the bare `pipeline-version.txt` older
  indexes published instead. `null` means the index records neither.
- `infection_status_summary.json`: per-host species promotion/demotion counts,
  uncovered counts, and override scope-gap counts.
- `params_changes.tsv`: compact top-level params changes (`key`, `kind`, `old`,
  `new`).
- `metadata_schema_summary.json`: counts of metadata columns added and removed.
- `metadata_schema_diff.tsv`: metadata column additions/removals (`change`,
  `column`). This is the only schema-diff output.

Then read the detailed TSVs needed by the template:

- `staleness.tsv` for §1 (`ref`, `current`, `current_date`, `latest`,
  `latest_date`, `status`). A `status` of `error` means the freshness check
  could not run; note the inability to verify in §1 and continue.
- `sizes.tsv`, `sizes_summary.json`, `params_changes.tsv`, `params_diff.txt`,
  `index_versions.json`, `metadata_schema_summary.json`, and
  `metadata_schema_diff.tsv` for §2 and §5.
  `sizes.tsv` is long-format (one row per `name`, `metric`): `metric == bytes`
  rows give per-entry byte sizes for the §2 size table; the content metrics
  (`records`, `total_bp`, `n_bp` for FASTAs; `rows` for TSVs) feed the §2 content
  findings and let you flag bytes moving opposite to content (e.g. bytes shrank
  while rows grew).
- `genomes_lost_categorized.tsv`, `genomes_gained_categorized.tsv`, `species_lost_all_genomes.tsv`, `species_gained_all_genomes.tsv`, and `genomes_reassigned.tsv` for §3 and appendices.
- `species_transitions_*.tsv` and `infection_status_transitions.tsv` for §4.

For appendix tables, paste Markdown tables generated from the TSV rows. For
large category tables, include the top rows requested by the template and state
the total row count in the appendix heading; do not rely on an external TSV path
as the table.

### Step 2a - Check metadata against the genome DB

An index's published metadata and its published genome DB FASTA should describe
the same set of genomes. Four counts in `genomes_summary.json` measure whether
they do, per side. The script restricts each index's metadata to the sequences
it ships before comparing, so a non-zero `metadata_rows_not_in_fasta_*` does not
distort the §3 deltas at all — but it is a fact about that index that belongs in
the report. A non-zero `fasta_ids_without_metadata_*` is different: it does
affect the deltas, for the IDs it counts. See the glossary below.

- `metadata_rows_not_in_fasta_old` — expected to be non-zero for any index built
  with pipeline version **3.2.2.0 or earlier** (check `index_versions.json`),
  which published metadata that was never reconciled to the genome DB; roughly a
  thousand rows on the last such index. Historical and benign: note it in §5 as
  the reason the old index's own metadata overstates its genome DB, and move on.
  Sanity-check the scale first, against that index's total metadata rows in
  `sizes.tsv` (the `rows` metric for `virus-genome-metadata-gid.tsv.gz`). A few
  percent is vintage drift; a large share is not. The script only rejects an ID
  mismatch when the metadata and the FASTA are *entirely* disjoint, so a partial
  namespace or accession-format change lands here looking like ordinary drift —
  treat an implausibly large count as that, not as vintage.
- `metadata_rows_not_in_fasta_new` — a finding. Either the candidate was also
  built with **3.2.2.0 or earlier**, or reconciliation has regressed. Say which,
  using `pipeline_version_new`. The candidate's published metadata describes
  genomes it does not ship, so anything reading that file as the genome set —
  including this script, before it restricts to DB membership — is wrong about
  the candidate.
- `fasta_ids_without_metadata_old` / `_new` — a defect on either side, whatever
  the vintage, and the more serious direction: RUN resolves `genome_id` to taxid
  through the metadata, so a sequence with no row is a reference RUN cannot
  attribute. Raise it in §5 and carry it into §Recommendations.

Report all four in §5 whenever any is non-zero, with the two pipeline versions
alongside so a reader can tell "expected for its vintage" from "regression".
The counts are measured from the index itself and are authoritative; the version
only sets the expectation. A `-dev` version is mid-cycle and settles nothing on
its own, so judge those indexes on the counts alone.

### Step 3 - Build §4 groupings

Use `infection_status_summary.json` for the §4 count table. The script keeps
deterministic coverage columns in the per-host TSVs but leaves presentation
grouping to the reviewer. Use the per-host `species_transitions_*.tsv` files
for evidence tables and narrative findings:

- Group actionable rows (`covered_by == ""`) by `(taxid, old_status, new_status)` to combine the same transition across hosts.
- Cross-reference actionable demotions against `species_lost_all_genomes.tsv` to note likely genome-loss-driven demotions.
- Flag any taxid that has both actionable `0->1` and `1->0` transitions across different hosts as bidirectional VHDB/taxonomy churn.
- Treat rows with non-empty `included_for_other_hosts` as override scope gaps.

### Step 4 - Optional VHDB cross-reference

Only needed when considering a candidate change for a `0->1` promotion or an
override policy gap. Fetch VHDB once:

```bash
curl -sL https://www.genome.jp/ftp/db/virushostdb/virushostdb.daily.tsv -o /tmp/vhdb-current.tsv
awk -F'\t' -v t=<taxid> '$1==t {print $1"\t"$2"\t"$6"\t"$8"\t"$9}' /tmp/vhdb-current.tsv
```

Columns: virus tax id, virus name, disease, host tax id, host name.

Notes:
- Homo sapiens taxid is 9606. If it is in column 4, VHDB lists this virus as human-infecting.
- If the taxid returns no rows, try grepping by name; freshly minted NCBI species IDs often lag VHDB's indexing, and the parent virus may carry annotations under a legacy taxid.
- Host taxid `1` (`root`) in column 4 means VHDB has the species but no specific host annotation; the demotion is real and no override is warranted on this evidence alone.

### Step 5 - Fill in the template

Copy `review-template.md` to `<outdir>/REVIEW.md` and fill it in. Read the
template's instructions for each section and follow them literally. Use
summary JSON/TSV files for counts and detailed TSVs for embedded evidence
tables.

Two reminders that are not obvious from the template alone:

- Trust the script's annotations. If a row has `covered_by_hard_exclude = 2169574`, the script verified the lineage; do not manually re-classify. The "Categorization buckets" glossary below explains what each script-emitted label means.
- Err on the side of inclusion in §Recommendations. Every plausible candidate change should appear, even at `low` confidence. In particular: any stale reference in §1, every uncovered promotion in §4, and every override policy gap in §4 should each appear as a candidate change.
- Sanity-check `rust_tools_version` in `params_changes.tsv`. It selects the Rust-tools container used to build the index and defaults to `main`, so a value of `main` on an index built from a `dev` checkout means the index was built with main's (older) Rust tools by accident — flag it in §5, and note that RUN must later use a matching version.

### Step 6 - Hand off

Print the `REVIEW.md` path back to the user. Do not open a PR or commit anything;
recommendations need human judgment before code review.

Do not `rm -rf` the outdir between iterations; the script overwrites its own
files, but `REVIEW.md` would go with the directory.

## Categorization buckets (glossary)

These are the labels the script writes into `genomes_lost_categorized.tsv`,
`genomes_gained_categorized.tsv`, and `genomes_summary.json` reason-count
objects. The template uses them as the §3.1 row labels.

"Lost" and "gained" throughout mean membership of the set of genomes an index
both ships *and* describes — its metadata restricted to the sequences present
in its own genome DB FASTA — not membership of the metadata alone. A genome an
index's metadata described but never shipped is therefore neither lost nor
gained when it disappears from the metadata, and one that enters the DB is a
gain even if both indexes' metadata always listed it.

"Ships and describes" and plain genome DB membership coincide unless an index
ships a sequence with no metadata row, which `fasta_ids_without_metadata_*`
counts. Such a sequence carries no
taxid or assembly for the categorizer to work from, so it cannot appear in
either table: if the old index described it and the new one does not, it is
reported lost even though the new index still ships it. When that count is
non-zero on either side, say so in §5 and treat the loss and gain figures as
unreliable for those specific IDs.

**Lost gid categories** (priority order; each gid gets the first applicable
bucket). The categorizer recovers each lost genome's build-time taxid and
`assembly_status` by joining its `assembly_accession` into the target index's
`virus-genome-metadata-raw.tsv.gz`, so attribution is exact. Assembly-lifecycle
reasons are checked before taxonomy/policy reasons, so the policy buckets mean
"current assemblies we stopped surveilling", not routine version churn:

- `absent_from_ncbi`: the lost genome's assembly is absent from the target index's raw metadata entirely.
- `non_current_genome_version`: assembly present in raw but its `assembly_status` is not `current`.
- `hard_excluded`: build-time leaf taxid or an ancestor is in `viral_taxids_exclude_hard` in the new build.
- `reassigned_to_excluded`: build-time leaf taxid differs from the old leaf taxid and the new leaf/species rollup is no longer surveilled.
- `infection_status_demotion`: build-time leaf taxid equals the old leaf taxid and the leaf/species rollup is no longer surveilled.
- `other`: present, current, surveilled, yet absent from the new genome DB — so the sequence was removed after its metadata row was written, which no bucket names. Sequence-header pattern exclusion (`ref/hv_patterns_exclude.txt`) is the mechanism today. Should be near zero; if not, investigate sequence-level filtering in `MAKE_VIRUS_GENOME_DB`.

**Gained gid categories** (priority order). Keyed on the genome's assigned leaf
taxon, using the assembly's `release_date` and `source_database` from the raw
table plus the old annotated DB:

- `newly_deposited`: assembly `release_date` is after the old index build.
- `hard_included`: the genome's new leaf taxon or an ancestor is in the `--new` index's published host-infection overrides.
- `new_taxon_in_taxonomy`: the genome's new leaf taxon did not exist in the old taxonomy DB.
- `infection_status_promotion`: the leaf taxon was in the old DB but not surveilled then and is now.
- `pre_existing_reincluded`: the assembly predates the old build and its taxon was already eligible, yet it was not in the old surveillance set. Break this down by `source_database` from `genomes_gained_categorized.tsv` and cross-reference params changes for likely drivers such as `assembly_source`.
- `no_release_date`: an eligible-taxon assembly with no `release_date`, so newly-deposited versus pre-existing cannot be decided.

Both lost and gained categorizers key on the genome's assigned **leaf** taxon,
never a species-rank rollup; the species rollup appears only inside the
surveillance predicate (leaf-positive OR species-positive), mirroring
`filter_viral_genbank_metadata.py`.

