#!/usr/bin/env bash

HF_USER="bel-bench"

declare -a corpora=(
    "ncbi-disease"
    "bc5cdr"
    "nlm-chem"
    "nlm-gene"
    "gnormplus"
    "s800"
    "linnaeus"
    "snp"
    "osiris"
    "tmvar3"
    "medmentions"
    "s1000"
    "biored"
)

for corpus in "${corpora[@]}"; do
    uv run python scripts/push_to_hub.py \
        --name "$corpus" \
        --type "corpus" \
        --repo "$HF_USER/$corpus" \
        --private
done



