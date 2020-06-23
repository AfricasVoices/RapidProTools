#!/bin/bash

set -e

IMAGE_NAME=fetch-raw-messages

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
if [[ $# -ne 3 ]]; then
    echo "Usage: ./docker-run-fetch-raw-messages.sh
    [--profile-memory <profile-output-path>]
    <domain> <token> <output-dir>"
    exit   
fi

# Assign the program arguments to bash variables.
DOMAIN=$1 
TOKEN=$2
OUTPUT_DIR=$3

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_MEMORY_PROFILER="$PROFILE_MEMORY" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_MEMORY" = true ]]; then
    PROFILE_MEMORY_CMD="mprof run -o /data/fetch_raw_messages_memory.prof"
fi

CMD="pipenv run $PROFILE_MEMORY_CMD python -u fetch_raw_messages.py \
    \"$DOMAIN\" \"$TOKEN\" /data/raw_messages.json
"

container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/. -> $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
docker cp "$container:/data/." "$OUTPUT_DIR"

if [[ "$PROFILE_MEMORY" = true ]]; then
    echo "Copying $container_short_id:/data/fetch_raw_messages_memory.prof -> $MEMORY_PROFILE_OUTPUT_PATH"
    mkdir -p "$(dirname "$MEMORY_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/fetch_raw_messages_memory.prof" "$MEMORY_PROFILE_OUTPUT_PATH"
fi
