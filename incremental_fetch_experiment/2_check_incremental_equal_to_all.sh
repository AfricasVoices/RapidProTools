#!/usr/bin/env bash

set -e

if [[ $# -ne 5 ]]; then
    echo "Usage: ./2_check_incremental_equal_to_all.sh <user> <server> <token> <flow> <data-dir>"
    echo "Downloads all messages from a flow and checks that the result is the same as the incremental fetch"
    exit
fi

USER=$1
SERVER=$2
TOKEN=$3
FLOW=$4
DATA_DIR=$5

echo "Fetching all runs..."
pipenv run python fetch_all.py "$USER" "$SERVER" "$TOKEN" "$FLOW" "$DATA_DIR"

echo "Coalescing incremental runs..."
pipenv run python coalesce_incremental_runs.py "$USER" "run_id - $FLOW" "$DATA_DIR/all_batched.json" "$DATA_DIR/all_coalesced.json"

echo "Comparing incremental vs. all runs fetch"
pipenv run python compare_runs.py "$DATA_DIR/all_coalesced.json" "$DATA_DIR/all_single_fetched.json"
