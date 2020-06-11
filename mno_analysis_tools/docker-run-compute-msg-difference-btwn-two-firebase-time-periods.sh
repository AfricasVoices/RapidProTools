#!/bin/bash

set -e

IMAGE_NAME=compute-msg-difference-btwn-two-firebase-time-periods

# Check that the correct number of arguments were provided.
if [[ $# -lt 6 ]]; then
    echo "Usage: ./docker-run-compute-msg-difference-btwn-periods.sh 
    <raw_messages_file_path> <target_operator> <target_message_direction> 
    <start_date> <end_date> <output_dir> <time_frame>" 
    exit   
fi
 
# Assign the program arguments to bash variables.
RAW_MESSAGES_FILE_PATH=$1
TARGET_OPERATOR=$2
TARGET_MESSAGE_DIRECTION=$3
START_DATE=$4
END_DATE=$5

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

if [[ $TARGET_MESSAGE_DIRECTION == "in" ]] 
then
   MSG_DIRECTION="incoming"
else
   MSG_DIRECTION="outgoing"
fi

if [[ -z $7 ]]; then
OUTPUT_DIR=$6
CMD="pipenv run python -u compute_msg_difference_btwn_two_firebase_time_periods.py \
    /data/raw_messages.json /data/${MSG_DIRECTION}_msg_diff_per_period.json \
    \"$TARGET_OPERATOR\" \"$TARGET_MESSAGE_DIRECTION\"  \"$START_DATE\" \"$END_DATE\" 
"
else
TIME_FRAME=$6
OUTPUT_DIR=$7
CMD="pipenv run python -u compute_msg_difference_btwn_two_firebase_time_periods.py \
    /data/raw_messages.json /data/${MSG_DIRECTION}_msg_diff_per_period.json \
    \"$TARGET_OPERATOR\" \"$TARGET_MESSAGE_DIRECTION\"  \"$START_DATE\" \"$END_DATE\" -t \"$TIME_FRAME\"
"
fi

container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$RAW_MESSAGES_FILE_PATH" "$container:/data/raw_messages.json"

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/. -> $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
docker cp "$container:/data/." "$OUTPUT_DIR"
