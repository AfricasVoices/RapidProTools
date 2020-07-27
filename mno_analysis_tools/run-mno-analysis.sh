#!/bin/bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-memory)
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            PROFILE_MEMORY_ARG="--profile-memory $MEMORY_PROFILE_OUTPUT_PATH"
            shift 2;;
        --time-frame)
            TIME_FRAME="$2"
            TIME_FRAME_ARG="--time-frame $TIME_FRAME"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 7 ]]; then
    echo "Usage: ./run-mno-analysis.sh [--profile-memory <profile-output-path>] [--time-frame <time-frame>]"
    echo " <domain> <token> <target_operator> <target_message_direction>"
    echo " <start_date> <end_date> <output_dir>"
    echo "Runs the Mno Analysis end-to-end (Fetch Raw Messages, compute window of downtime, 
        compute message difference between two firebase periods)"
    exit
fi
    
DOMAIN=$1 
TOKEN=$2
TARGET_OPERATOR=$3
TARGET_MESSAGE_DIRECTION=$4
START_DATE=$5
END_DATE=$6
OUTPUT_DIR=$7

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"

echo "Starting run with id '$RUN_ID'"

./docker-run-fetch-raw-messages.sh ${PROFILE_MEMORY_ARG} "$DOMAIN" "$TOKEN" "$OUTPUT_DIR"

./docker-run-compute-window-of-downtime.sh ${PROFILE_MEMORY_ARG} "${OUTPUT_DIR%/}/raw_messages.json" \
    "$TARGET_OPERATOR" "$TARGET_MESSAGE_DIRECTION" "$START_DATE" "$END_DATE" "$OUTPUT_DIR"

./docker-run-compute-msg-difference-btwn-two-firebase-time-periods.sh ${PROFILE_MEMORY_ARG} ${TIME_FRAME_ARG} "${OUTPUT_DIR%/}/raw_messages.json" \
    "$TARGET_OPERATOR" "$TARGET_MESSAGE_DIRECTION" "$START_DATE" "$END_DATE" "$OUTPUT_DIR"
