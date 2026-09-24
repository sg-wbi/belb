#!/usr/bin/env bash

HF_USER="bel-bench"

declare -a kbs=(
    "mesh"
    "mondo"
    "cellosaurus"
    "ncbi-taxonomy"
    "ncbi-gene"
)

for kb in "${kbs[@]}"; do
    uv run python scripts/push_to_hub.py \
        --name "$kb" \
        --type "kb" \
        --repo "$HF_USER/$kb"
done

declare -a kbs=(
    "ctd-diseases"
    "ctd-chemicals"
    "umls"
)

for kb in "${kbs[@]}"; do
    uv run python scripts/push_to_hub.py \
        --name "$kb" \
        --type "kb" \
        --repo "$HF_USER/$kb" \
        --private
done
