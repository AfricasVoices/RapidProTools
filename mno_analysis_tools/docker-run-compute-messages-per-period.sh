#!/bin/bash

set -e

IMAGE_NAME=compute-messages-per-period

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
if [[ $# -ne 7 ]]; then
    echo "Usage: ./docker-run-compute-window-of-downtime.sh 
    <raw_messages_file_path> <target_operator> <target_message_direction> 
    <start_date> <end_date> <time_frame> <output_dir>" 
    exit   
fi
 
# Assign the program arguments to bash variables.
RAW_MESSAGES_FILE_PATH=$1
TARGET_OPERATOR=$2
TARGET_MESSAGE_DIRECTION=$3
START_DATE=$4
END_DATE=$5
TIME_FRAME=$6
OUTPUT_DIR=$7

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

CMD="pipenv run $PROFILE_MEMORY_CMD python -u compute_messages_per_period.py \
    /data/raw_messages.json /data/${MSG_DIRECTION}_msg.json \
    \"$TARGET_OPERATOR\" \"$TARGET_MESSAGE_DIRECTION\"  \"$START_DATE\" \"$END_DATE\" \"$TIME_FRAME\"
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
