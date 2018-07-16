import argparse
import csv
import math
from datetime import timedelta, datetime

from temba_client.v2 import TembaClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build a CSV of the total number of messages received at the"
                                                 "given time interval")
    parser.add_argument("server", help="Base URL of server channel to connect to, including c/ex/<uuid>/", nargs=1)
    parser.add_argument("api_token", metavar="api-token", help="Rapid Pro API Token", nargs=1)

    args = parser.parse_args()
    server = args.server[0]
    api_token = args.api_token[0]

    rapid_pro = TembaClient(server, api_token)

    # Download all messages sent to Rapid Pro.
    folders = ["inbox", "flows"]
    messages = []
    for folder in folders:
        messages += rapid_pro.get_messages(folder=folder).all()

    # Produce a CSV which can be easily graphed in e.g. Excel:
    # Bucket messages by sent_on in time ranges of 1 hr
    sent_interval = timedelta(hours=1)
    counts = dict()
    for message in messages:
        bucket = math.floor(message.created_on.timestamp() / sent_interval.total_seconds())
        if bucket not in counts:
            counts[bucket] = 0
        counts[bucket] += 1

    # Fill in missing times in range with 0-counts.
    start_time = min(counts.keys())
    end_time = max(counts.keys())
    for time in range(start_time, end_time):
        if time not in counts:
            counts[time] = 0

    data = [{"time": k, "count": v} for k, v in counts.items()]
    data.sort(key=lambda x: x["time"])
    for x in data:
        x["time"] = datetime.fromtimestamp(x["time"] * sent_interval.total_seconds()).isoformat()

    with open("test.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=["time", "count"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)
