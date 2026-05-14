#!/usr/bin/env bash
# Launches one cohort of the CONCATENATE_GENOME_FASTA benchmark.
# Usage: bench/launch.sh <cohort>   # cohort ∈ {single, xsmall, small, large}
# Must be run from the repo root.

set -euo pipefail

cohort="$1"
repo_root="$(pwd)"
config="${repo_root}/bench/cohort_${cohort}.config"
base_dir="s3://nao-jo/concat-bench-par/${cohort}"
launch_dir="${repo_root}/bench/runs/${cohort}"

if [[ ! -f "$config" ]]; then
    echo "Missing cohort config: $config" >&2
    exit 1
fi

mkdir -p "${launch_dir}"
cd "${launch_dir}"

echo "=== Launching cohort=${cohort} ===" | tee "launch_${cohort}.log"
echo "config=${config}" | tee -a "launch_${cohort}.log"
echo "base_dir=${base_dir}" | tee -a "launch_${cohort}.log"
echo "launch_dir=${launch_dir}" | tee -a "launch_${cohort}.log"

nextflow run "${repo_root}/concat_bench.nf" \
    -c "${config}" \
    --base_dir "${base_dir}" \
    -profile standard \
    -work-dir "${base_dir}/work" \
    2>&1 | tee -a "launch_${cohort}.log"
