#!/bin/bash

set -e

IMAGE_NAME=rapid-pro-fetch-runs

while [[ $# -gt 0 ]]; do
    case "$1" in
        --flow-name)
            FLOW_NAME="--flow-name $2"
            shift 2;;
        --range-start-inclusive)
            RANGE_START_INCLUSIVE="--range-start-inclusive $2"
            shift 2;;
        --range-end-exclusive)
            RANGE_END_EXCLUSIVE="--range-end-exclusive $2"
            shift 2;;
        --test-contacts-path)
            TEST_CONTACTS="--test-contacts-path /data/input-test-contacts.json"
            TEST_CONTACTS_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [ $# -ne 6 ]; then
    echo "Usage: sh docker-run.sh
    --flow-name <flow-name> --test-contacts-path <test-contacts-path>
    --range-start-inclusive <range-start-inclusive> --range-end-exclusive <range-end-exclusive>
    <server> <token> <user> {all,latest-only} <phone-uuid-table> <output-json>"
    exit
fi

# Assign the program arguments to bash variables.
SERVER=$1
TOKEN=$2
USER=$3
MODE=$4
PHONE_UUID_TABLE=$5
OUTPUT_JSON=$6

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
container="$(docker container create --env TEST_CONTACTS="$TEST_CONTACTS" --env RANGE_START_INCLUSIVE="$RANGE_START_INCLUSIVE" --env RANGE_END_EXCLUSIVE="$RANGE_END_EXCLUSIVE" --env SERVER="$SERVER" --env TOKEN="$TOKEN" --env FLOW_NAME="$FLOW_NAME" --env USER="$USER" --env MODE="$MODE" "$IMAGE_NAME")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$PHONE_UUID_TABLE" "$container:/data/phone-uuid-table.json"
if ! [ -z "$TEST_CONTACTS_PATH" ]; then
    docker cp "$TEST_CONTACTS_PATH" "$container:/data/input-test-contacts.json"
fi

# Run the image as a container.
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$PHONE_UUID_TABLE")"
docker cp "$container:/data/phone-uuid-table.json" "$PHONE_UUID_TABLE"

mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"
