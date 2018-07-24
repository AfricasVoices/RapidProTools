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

# Copy the rest of the project
ADD fetch_runs.py /app

# Make a directory for intermediate data
RUN mkdir /data

# Environment variables need to be set when constructing this container e.g. via
# docker run or docker container create. Use docker-run.sh to set these automatically.
CMD pipenv run python fetch_runs.py --server "$SERVER" "$TOKEN" --flow-name "$FLOW_NAME" "$USER" "$MODE" /data/phone-uuid-table.json /data/output.json
