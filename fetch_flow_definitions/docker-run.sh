#!/bin/bash

set -e

IMAGE_NAME=rapid-pro-fetch-flow-definitions

# Check that the correct number of arguments were provided.
if [[ $# -lt 4 ]]; then
    echo "Usage: sh docker-run.sh
    <server> <token> <flow-name> [<flow-name> ...] <output-json>"
    exit
fi

# Assign the program arguments to bash variables.
SERVER=$1
TOKEN=$2

shift 2
FLOW_NAMES=""
while [[ $# -gt 1 ]]; do
    FLOW_NAMES="$FLOW_NAMES \"$1\""
    shift 1
done

#FLOW_NAMES=(${@:3:$(($#-3))}) # All arguments but first 2 and last
OUTPUT_JSON=$1

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u fetch_flow_definitions.py \"$SERVER\" \"$TOKEN\" $FLOW_NAMES /data/output.json"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Run the image as a container.
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"
