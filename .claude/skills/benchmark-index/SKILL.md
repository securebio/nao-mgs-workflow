---
name: benchmark-index
description: Compare two mgs-workflow index releases and produce a structured pre-rollout review report. Runs `bin/benchmark_index.py` (with `--repo-root` so it can annotate transitions with existing rule coverage, classify lost / gained genome IDs by reason, and check Kraken2 / SILVA reference freshness), then fills `review-template.md` with data from `facts.json` and the TSV outputs to produce a standalone `REVIEW.md`. Use before promoting a new `s3://nao-mgs-index/<DATE>` build to production.
---

# Benchmark an index release

The point of the report is to help the reviewer decide whether to ship this
index as-is or change something first. The script does deterministic data
extraction; you fill in `review-template.md` from `facts.json` and the TSVs.

**`review-template.md` is the source of truth for report structure.** Open it,
read it, fill it in literally. This skill provides procedural support and a
small glossary of script-specific category labels. When the template and the
skill seem to disagree, the template wins.

`REVIEW.md` must be standalone. Use the script outputs as inputs, but do not
leave the final report saying "see facts.json" or "see species_transitions.tsv"
instead of embedding the needed table or fact. Appendix tables can be embedded
naively from TSVs; polish is less important than keeping the report self-contained.

## When to use

- The user wants to vet a new index release before promoting it to production.
- The user has two `s3://nao-mgs-index/<DATE>` URIs (or local paths) and asks for a comparison.
- The user references "index benchmark", "index review", "index rollout check", or similar.

If the user is only asking for raw numbers (no written review), run the script
and surface `<outdir>/facts.json` plus the output directory; do not write
`REVIEW.md`.

## Inputs

- `--old <root>`: parent of `output/` for the old (reference) index. `s3://...` or local path. **Required.**
- `--new <root>`: parent of `output/` for the new (target) index. **Required.**
- `--out <dir>`: output directory. Use an absolute path so paths are reader-portable. **Required.**
- `--repo-root <path>`: a mgs-workflow checkout. Optional in the script, but in practice always pass `--repo-root .` from a checkout. Without it the script skips coverage annotations, and the Edge cases section becomes load-bearing.

If `--old`, `--new`, or `--out` is missing, ask the user; do not guess.

## Procedure

### Step 1 - Run the script

`cd` into a mgs-workflow checkout, then:

```bash
cd /path/to/mgs-workflow
python bin/benchmark_index.py \
  --old <old> --new <new> \
  --out <outdir> --repo-root .
```

Use **absolute** paths for `--out` (e.g. `/tmp/bench-...`). Takes about 60
seconds. If `<outdir>/facts.json` already exists, reuse it only if the user
asked you to avoid rerunning; otherwise rerun so the reference freshness checks
are current.

### Step 2 - Read facts and TSVs

Read `<outdir>/facts.json` first. It contains compact counts and filenames:
Kraken2/SILVA staleness rows, size-change counts, metadata schema changes,
lost/gained genome reason counts, taxonomy counts, per-host infection-status
counts, and top-level params changes.

Then read the detailed TSVs needed by the template:

- `sizes.tsv`, `params_diff.txt`, and `facts.json.params.changes` for §2 and §5. `sizes.tsv` is long-format (one row per `name`, `metric`): rows with `metric == bytes` are the per-entry byte sizes for the §2 size table; the other metrics are content metrics for the §2 content findings (`records`, `total_bp`, `n_bp` for each FASTA output; `rows` for each TSV output, for every FASTA/TSV present in both indexes). These let you flag cases where compressed bytes moved opposite to actual content (e.g. bytes shrank while row count grew).
- `genomes_lost_categorized.tsv`, `genomes_gained_categorized.tsv`, `species_lost_all_genomes.tsv`, `species_gained_all_genomes.tsv`, and `genomes_reassigned.tsv` for §3 and appendices.
- `species_transitions_*.tsv` and `infection_status_transitions.tsv` for §4.

For appendix tables, paste Markdown tables generated from the TSV rows. For
large category tables, include the top rows requested by the template and state
the total row count in the appendix heading; do not rely on an external TSV path
as the table.

### Step 3 - Build §4 groupings

The script keeps deterministic coverage columns but leaves presentation grouping
to the reviewer. Use the per-host `species_transitions_*.tsv` files:

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
`facts.json` for counts and TSVs for embedded evidence tables.

Two reminders that are not obvious from the template alone:

- Trust the script's annotations. If a row has `covered_by_hard_exclude = 2169574`, the script verified the lineage; do not manually re-classify. The "Categorization buckets" glossary below explains what each script-emitted label means.
- Err on the side of inclusion in §Recommendations. Every plausible candidate change should appear, even at `low` confidence. In particular: any stale reference in §1, every uncovered promotion in §4, and every override policy gap in §4 should each appear as a candidate change.

### Step 6 - Hand off

Print the `REVIEW.md` path back to the user. Do not open a PR or commit anything;
recommendations need human judgment before code review.

Do not `rm -rf` the outdir between iterations; the script overwrites its own
files, but `REVIEW.md` would go with the directory.

## Categorization buckets (glossary)

These are the labels the script writes into `genomes_lost_categorized.tsv`,
`genomes_gained_categorized.tsv`, and `facts.json.genomes.*_by_reason`. The
template uses them as the §3.1 row labels.

**Lost gid categories** (priority order; each gid gets the first applicable
bucket). The raw categorizer recovers each lost genome's build-time taxid and
assembly_status by joining its `assembly_accession` into the target index's
`virus-genome-metadata-raw.tsv.gz`, so attribution is exact: no NCBI lookups and
no no-drift assumption. Assembly-lifecycle reasons are checked before
taxonomy/policy reasons, so the policy buckets mean "current assemblies we
stopped surveilling" and are not inflated by routine version churn:

- `absent_from_ncbi`: the lost genome's assembly is absent from the target index's raw metadata entirely.
- `non_current_genome_version`: assembly present in raw but its `assembly_status` is not `current`.
- `hard_excluded`: build-time species_taxid or an ancestor is in `viral_taxids_exclude_hard` in the new build.
- `reassigned_to_excluded`: build-time species_taxid differs from the old species_taxid and the new species is no longer surveilled.
- `infection_status_demotion`: build-time species_taxid equals the old species_taxid and the species is no longer surveilled.
- `other`: present, current, surveilled, yet absent from the new gid set. Should be near zero; if not, investigate downstream sequence-level filtering.

The target index must publish `virus-genome-metadata-raw.tsv.gz`. If it is
missing, the script errors out rather than guessing.

**Gained gid categories** (priority order). Keyed on the genome's assigned leaf
taxon, using the assembly's `release_date` and `source_database` from the raw
table plus the old annotated DB:

- `newly_deposited`: assembly `release_date` is after the old index build.
- `hard_included`: the genome's new leaf taxon or an ancestor is in `ref/host-infection-overrides.json`.
- `new_taxon_in_taxonomy`: the genome's new leaf taxon did not exist in the old taxonomy DB.
- `infection_status_promotion`: the leaf taxon was in the old DB but not surveilled then and is now.
- `pre_existing_reincluded`: the assembly predates the old build and its taxon was already eligible, yet it was not in the old surveillance set. Break this down by `source_database` from `genomes_gained_categorized.tsv` and cross-reference params changes for likely drivers such as `assembly_source`.
- `no_release_date`: an eligible-taxon assembly with no `release_date`, so newly-deposited versus pre-existing cannot be decided.

Both lost and gained categorizers key on the genome's assigned **leaf** taxon,
never a species-rank rollup; the species rollup appears only inside the
surveillance predicate (leaf-positive OR species-positive), mirroring
`filter_viral_genbank_metadata.py`.

## Edge cases

- **`--repo-root` skipped**: the script falls back to no coverage annotation. Re-run with `--repo-root .` from a mgs-workflow checkout.
- **A reference check errors out**: `facts.json.staleness[*].status` is `error` for that row. Note the inability to verify in §1 and continue.
- **Empty `genomes_added` / `genomes_removed`**: §3.1 categorized counts are zero; §3.2 / §3.3 collapse to "No genome IDs lost/gained".
