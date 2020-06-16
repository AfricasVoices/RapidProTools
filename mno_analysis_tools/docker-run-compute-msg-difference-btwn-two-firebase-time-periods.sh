#!/bin/bash

set -e

IMAGE_NAME=compute-msg-difference-btwn-two-firebase-time-periods

while [[ $# -gt 0 ]]; do
    case "$1" in
         --profile-memory)
            PROFILE_MEMORY=true
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

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
OUTPUT_DIR=$6
TIME_FRAME=${7:-00:00:10}

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_MEMORY_PROFILER="$PROFILE_MEMORY" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_MEMORY" = true ]]; then
    PROFILE_MEMORY_CMD="mprof run -o /data/memory.prof"
fi

if [[ $TARGET_MESSAGE_DIRECTION == "in" ]] 
then
   MSG_DIRECTION="incoming"
else
   MSG_DIRECTION="outgoing"
fi

CMD="pipenv run $PROFILE_MEMORY_CMD python -u compute_msg_difference_btwn_two_firebase_time_periods.py \
    /data/raw_messages.json /data/${MSG_DIRECTION}_msg_diff_per_period.json \
    \"$TARGET_OPERATOR\" \"$TARGET_MESSAGE_DIRECTION\"  \"$START_DATE\" \"$END_DATE\" -t \"$TIME_FRAME\"
"

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
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/. -> $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
docker cp "$container:/data/." "$OUTPUT_DIR"

if [[ "$PROFILE_MEMORY" = true ]]; then
    echo "Copying $container_short_id:/data/memory.prof -> $MEMORY_PROFILE_OUTPUT_PATH"
    mkdir -p "$(dirname "$MEMORY_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/memory.prof" "$MEMORY_PROFILE_OUTPUT_PATH"
fi
