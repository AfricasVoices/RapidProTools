FROM python:3.6-slim

# Install the tools we need.
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile.lock /app
ADD Pipfile /app
RUN pipenv sync

# Make a directory for intermediate data
RUN mkdir /data

# Copy the rest of the project
ADD fetch_runs.py /app

# Environment variables need to be set when constructing this container e.g. via
# docker run or docker container create. Use docker-run.sh to set these automatically.
CMD pipenv run python -u fetch_runs.py --server "$SERVER" "$TOKEN" $FLOW_NAME $RANGE_START_INCLUSIVE $RANGE_END_EXCLUSIVE \
    $TEST_CONTACTS "$USER" "$MODE" /data/phone-uuid-table.json /data/output.json
