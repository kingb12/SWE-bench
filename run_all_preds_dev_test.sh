#!/bin/bash

# Check if argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <folder>"
    exit 1
fi

# Check if the folder exists
if [ ! -d "$1" ]; then
    echo "Error: '$1' is not a valid directory"
    exit 1
fi

# Find all preds.json files and run the evaluation script on each
find "$1" -name "preds.json" -type f | while read -r pred_file; do
    echo "Processing: $pred_file"
    ~/SWE-bench/run_k8s_eval_dev_test.sh "$pred_file"
    echo "Finished: $pred_file"
    echo "---"
done
