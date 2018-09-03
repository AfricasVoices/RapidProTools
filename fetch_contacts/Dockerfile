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
ADD fetch_contacts.py /app

# Make a directory for intermediate data
RUN mkdir /data

# Environment variables need to be set when constructing this container e.g. via
# docker run or docker container create. Use docker-run.sh to set these automatically.
CMD pipenv run python fetch_contacts.py $TEST_CONTACTS_ARG --server "$SERVER" "$TOKEN" "$USER" \
    /data/phone-uuid-table.json /data/output.json
