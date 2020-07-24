#!/bin/bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
         --optional-time-frame)
            OPTIONAL_TIME_FRAME=true
            OPTIONAL_TIME_FRAME_VALUE="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 9 ]]; then
    echo "Usage: ./run-mno-analysis.sh"
    echo " [--optional-time-frame <optional_time_frame>]"
    echo " <domain> <token> <raw_messages_file_path>"
    echo " <target_operator> <target_message_direction>"
    echo " <start_date> <end_date> <time_frame>"
    echo " <output_dir>"
    echo "Runs the Mno Analysis end-to-end (Fetch Raw Messages, compute window of downtime,
        compute messages per period, compute msg difference btwn periods)"
    exit
fi
    
DOMAIN=$1 
TOKEN=$2
RAW_MESSAGES_FILE_PATH=$3
TARGET_OPERATOR=$4
TARGET_MESSAGE_DIRECTION=$5
START_DATE=$6
END_DATE=$7
TIME_FRAME=$8
OUTPUT_DIR=$9

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"

echo "Starting run with id '$RUN_ID'"

./docker-run-fetch-raw-messages.sh --profile-memory ./data "$DOMAIN" "$TOKEN" "$OUTPUT_DIR"

./docker-run-compute-window-of-downtime.sh --profile-memory ./data "$RAW_MESSAGES_FILE_PATH" \
    "$TARGET_OPERATOR" "$TARGET_MESSAGE_DIRECTION" "$START_DATE" "$END_DATE" "$OUTPUT_DIR"

./docker-run-compute-msg-difference-btwn-two-firebase-time-periods.sh --profile-memory ./data "$RAW_MESSAGES_FILE_PATH" \
    "$TARGET_OPERATOR" "$TARGET_MESSAGE_DIRECTION" "$START_DATE" "$END_DATE" "$OUTPUT_DIR"
