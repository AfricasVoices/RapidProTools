FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD fetch_raw_messages.py /app
ADD compute_window_of_downtime.py /app
ADD compute_messages_per_period.py /app
ADD compute_msg_difference_btwn_periods.py /app
