#! /usr/bin/env bash
# Removes a retired dataset's rows from Postgres. Only counts them unless DRY_RUN is "false".
set -e

if [[ -z "$DATASET_NAME" ]]; then
    echo "DATASET_NAME is not set"
    exit 1
fi

echo "Retiring $DATASET_NAME rows from postgres (DRY_RUN=${DRY_RUN:-true})"
python3 -m pgload.retire --dataset "$DATASET_NAME" --dry-run "${DRY_RUN:-true}"
