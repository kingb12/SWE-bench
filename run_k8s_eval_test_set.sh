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

# Get directory part (remove trailing filename like preds.json)
BASE_PATH=$(dirname "$PRED_PATH")

# Sanitize: lowercase, replace invalid chars with '-', and trim leading/trailing '-'
SANITIZED=$(echo "$BASE_PATH" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | sed -E 's/^-+//; s/-+$//')

# Compute 8-character hash
HASH=$(echo -n "$PRED_PATH" | sha1sum | cut -c1-8)

# Take the last 50 characters
LAST50=${SANITIZED: -50}

# Combine into run_id
RUN_ID="k8s-${HASH}-${LAST50}"

# Trim any leading/trailing dashes (just to be safe)
RUN_ID=$(echo "$RUN_ID" | sed -E 's/^-+//; s/-+$//')

echo "Derived run_id: $RUN_ID"

/Users/bking/SWE-bench/venv/bin/python -m swebench.harness.run_evaluation \
    --dataset_name "princeton-nlp/SWE-bench_Verified" \
    --split "test" \
    --predictions_path "$PRED_PATH" \
    --max_workers 10 \
    --run_id "$RUN_ID" \
    --kubernetes true \
    --namespace swebench \
    --kubernetes_namespace jlab-nlp \
    --report_path "${PRED_PATH/preds.json/sb_eval_report.json}"
    "$@"

