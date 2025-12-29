#!/usr/bin/env bash

# Given an input directory of *_validation_hits.tsv.gz files, subset them in
# parallel to records with LCA assignments within one of the provided target
# clades.

if [[ $# -lt 5 ]]; then
    echo "Usage: $0 <indir> <outdir> <jobs> <index> <taxid1> [taxid2 [taxid3 ...]]"
    exit 1
fi

IN_DIR="$1"
shift

OUT_DIR="$1"
shift

JOBS="$1"
shift

INDEX_FNAME="$1"
shift

TARGET_CLADES="$@"

for in_fname in "$IN_DIR"/*validation_hits.tsv.gz; do
    base=$(basename "$in_fname")
    out_fname="$OUT_DIR/$base"
    if [[ -e "$out_fname" ]] ; then
        continue
    fi

    echo $base
done | xargs -P "$JOBS" -I {} \
  ./subset-validation-hits-by-clade.py \
       "$IN_DIR"/{} "$OUT_DIR"/{} "$INDEX_FNAME" $TARGET_CLADES
