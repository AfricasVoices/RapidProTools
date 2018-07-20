#!/bin/bash

set -e

IMAGE_NAME=rapid-pro-fetch-runs

# Check that the correct number of arguments were provided.
if [ $# -ne 7 ]; then
    echo "Usage: sh docker-run.sh <server> <token> <flow-name> <user> {all,latest-only} <phone-uuid-table> <output-json>"
    exit
fi

# Assign the program arguments to bash variables.
SERVER=$1
TOKEN=$2
FLOW_NAME=$3
USER=$4
MODE=$5
PHONE_UUID_TABLE=$6
OUTPUT_JSON=$7

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
container="$(docker container create --env SERVER="$SERVER" --env TOKEN="$TOKEN" --env FLOW_NAME="$FLOW_NAME" --env USER="$USER" --env MODE="$MODE" "$IMAGE_NAME")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$PHONE_UUID_TABLE" "$container:/data/phone-uuid-table.json"

# Run the image as a container.
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$PHONE_UUID_TABLE")"
docker cp "$container:/data/phone-uuid-table.json" "$PHONE_UUID_TABLE"

mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"
