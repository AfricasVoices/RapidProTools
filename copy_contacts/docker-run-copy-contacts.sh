#!/bin/bash

set -e

IMAGE_NAME=copy-contacts

# Check that the correct number of arguments were provided.
if [[ $# -ne 5 ]]; then
    echo "Usage: ./docker-run-copy-contacts.sh
    <google-cloud-credentials-file-path> <source-domain> <source-credentials-url> <target-domain>
    <target-credentials-url>"
    exit
fi

# Assign the program arguments to bash variables.
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$1
SOURCE_DOMAIN=$2
SOURCE_CREDENTIALS_URL=$3
TARGET_DOMAIN=$4
TARGET_DOMAIN_URL=$5

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

CMD="pipenv run python -u copy_contacts.py \
    /credentials/google-cloud-credentials.json \
    \"$SOURCE_DOMAIN\" \"$SOURCE_CREDENTIALS_URL\" \
    \"$TARGET_DOMAIN\" \"$TARGET_DOMAIN_URL\"
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
docker start -a -i "$container"
