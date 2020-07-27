FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Install memory_profiler if this script is run with PROFILE_MEMORY flag
ARG INSTALL_MEMORY_PROFILER="false"
RUN if [ "$INSTALL_MEMORY_PROFILER" = "true" ]; then \
        apt-get update && apt-get install -y gcc && \
        pip install memory_profiler; \
    fi

# Make a directory for intermediate data
RUN mkdir /data
RUN mkdir /system-metrics

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD fetch_raw_messages.py /app
ADD compute_window_of_downtime.py /app
ADD compute_msg_difference_btwn_two_firebase_time_periods.py /app
