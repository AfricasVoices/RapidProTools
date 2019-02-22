#!/usr/bin/env bash

set -e

if [[ $# -ne 5 ]]; then
    echo "Usage: ./1_run_incremental_fetch.sh <user> <server> <token> <flow> <data-dir>"
    echo "Incrementally fetches runs from a given flow on loop"
    exit
fi

USER=$1
SERVER=$2
TOKEN=$3
FLOW=$4
DATA_DIR=$5

while :; do
    pipenv run python incremental_fetch.py "$USER" "$SERVER" "$TOKEN" "$FLOW" "$DATA_DIR"
done
