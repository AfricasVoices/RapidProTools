#!/bin/bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
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

if [[ $# -ne 8 ]]; then
    echo "Usage: ./run-mno-analysis.sh [--time-frame <time-frame>]"
    echo " <domain> <token> <raw_messages_file_path>"
    echo " <target_operator> <target_message_direction>"
    echo " <start_date> <end_date>"
    echo " <output_dir>"
    echo "Runs the Mno Analysis end-to-end (Fetch Raw Messages, compute window of downtime, 
        compute message difference between two firebase periods)"
    exit
fi
    
DOMAIN=$1 
TOKEN=$2
RAW_MESSAGES_FILE_PATH=$3
TARGET_OPERATOR=$4
TARGET_MESSAGE_DIRECTION=$5
START_DATE=$6
END_DATE=$7
OUTPUT_DIR=$8

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"

echo "Starting run with id '$RUN_ID'"

./docker-run-fetch-raw-messages.sh --profile-memory ./data "$DOMAIN" "$TOKEN" "$OUTPUT_DIR"

./docker-run-compute-window-of-downtime.sh --profile-memory ./data "$RAW_MESSAGES_FILE_PATH" \
    "$TARGET_OPERATOR" "$TARGET_MESSAGE_DIRECTION" "$START_DATE" "$END_DATE" "$OUTPUT_DIR"

./docker-run-compute-msg-difference-btwn-two-firebase-time-periods.sh --profile-memory ./data ${TIME_FRAME_ARG} "$RAW_MESSAGES_FILE_PATH" \
    "$TARGET_OPERATOR" "$TARGET_MESSAGE_DIRECTION" "$START_DATE" "$END_DATE" "$OUTPUT_DIR"
