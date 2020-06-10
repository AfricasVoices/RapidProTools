#!/bin/bash

set -e

IMAGE_NAME=fetch-raw-messages

# Check that the correct number of arguments were provided.
if [[ $# -ne 3 ]]; then
    echo "Usage: ./docker-run-fetch-raw-messages.sh <domain> <token> <output-dir>"
    exit   
fi

# Assign the program arguments to bash variables.
DOMAIN=$1 
TOKEN=$2
OUTPUT_DIR=$3

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

CMD="pipenv run python -u fetch_raw_messages.py \
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
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/. -> $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
docker cp "$container:/data/." "$OUTPUT_DIR"
