#!/usr/bin/env bash
# Launches one cohort of the ADD_GENBANK_GENOME_IDS benchmark.
# Usage: bench/launch_addgenome.sh <cohort>   # cohort ∈ {single, xsmall, small, large}
# Must be run from the repo root.

set -euo pipefail

cohort="$1"
repo_root="$(pwd)"
config="${repo_root}/bench/cohort_${cohort}.config"
base_dir="s3://nao-jo/addgenome-bench-par/${cohort}"
launch_dir="${repo_root}/bench/runs-addgenome/${cohort}"

if [[ ! -f "$config" ]]; then
    echo "Missing cohort config: $config" >&2
    exit 1
fi

mkdir -p "${launch_dir}"
cd "${launch_dir}"

echo "=== Launching addgenome cohort=${cohort} ===" | tee "launch_${cohort}.log"
echo "config=${config}" | tee -a "launch_${cohort}.log"
echo "base_dir=${base_dir}" | tee -a "launch_${cohort}.log"

# Inputs from the production ADD_GENBANK_GENOME_IDS work dir for the
# Riboviria-shard build (matches what we used for CONCATENATE_GENOME_FASTA).
METADATA="s3://nao-jo/mgs-index/work/4a/36ac0bfbc7efe48d6119b17481bed2/virus-genome-metadata-filtered.tsv.gz"
GENOMES_DIR="s3://nao-jo/mgs-index/work/4a/36ac0bfbc7efe48d6119b17481bed2/ncbi_genomes"

nextflow run "${repo_root}/addgenome_bench.nf" \
    -c "${config}" \
    --base_dir "${base_dir}" \
    --metadata "${METADATA}" \
    --genomes_dir "${GENOMES_DIR}" \
    -profile standard \
    -work-dir "${base_dir}/work" \
    2>&1 | tee -a "launch_${cohort}.log"
