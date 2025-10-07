#!/bin/bash
# run_eval.sh
# Usage: ./run_eval.sh <predictions_path> [extra args...]

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <predictions_path> [extra args...]"
    exit 1
fi

PRED_PATH="$1"

shift  # remove first argument

# Expand ~ to full path

# Remove trailing /preds.json or similar
BASE_PATH=$(dirname "$PRED_PATH")

# Replace invalid chars with '-', lowercase, and trim
SANITIZED=$(echo "$BASE_PATH" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | sed -E 's/^-+//; s/-+$//')

# Take the last 64 chars (ensuring alphanumeric start/end)
RUN_ID="k8s-${SANITIZED: -58}"
RUN_ID=$(echo "$RUN_ID" | sed -E 's/^-+//; s/-+$//')

echo "Derived run_id: $RUN_ID"

# Optional: echo the final run id for debugging
echo "Derived run_id: $RUN_ID"

/Users/bking/SWE-bench/venv/bin/python -m swebench.harness.run_evaluation \
    --dataset_name "Brendan/SWE-bench_Verified" \
    --split "dev" \
    --predictions_path "$PRED_PATH" \
    --max_workers 32 \
    --run_id "$RUN_ID" \
    --kubernetes true \
    --namespace swebench \
    --kubernetes_namespace jlab-nlp \
    "$@"

