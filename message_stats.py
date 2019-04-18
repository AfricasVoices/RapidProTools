import argparse
import datetime
import json

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger
from dateutil.parser import isoparse

from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("server")
parser.add_argument("token")
parser.add_argument("date")

args = parser.parse_args()
server = args.server
token = args.token
date = isoparse(args.date)

rapid_pro = RapidProClient(server, token)

# Get messages last updated today
raw_messages = rapid_pro \
    .get_raw_messages(created_after_inclusive=date, created_before_exclusive=date + datetime.timedelta(days=1))\
    .all(retry_on_rate_exceed=True)
log.info(f"Fetched {len(raw_messages)} raw messages last modified since {date.isoformat()}")

log.info("Counting the number of messages in/out/failed per operator...")
operator_counts = dict()  # of operator -> category -> count
for msg in raw_messages:
    phone_number = msg.urn.replace("tel:", "")
    operator = PhoneCleaner.clean_operator(phone_number)

    if operator not in operator_counts:
        operator_counts[operator] = {
            "incoming": 0,
            "outgoing": 0,
            "outgoing failures": 0
        }

    if msg.direction == "in":
        operator_counts[operator]["incoming"] += 1
    elif msg.direction == "out":
        if msg.status == "errored":
            operator_counts[operator]["outgoing failures"] += 1
        elif msg.status == "wired":
            operator_counts[operator]["outgoing"] += 1
        else:
            print(json.dumps(msg.serialize(), indent=2))
    else:
        print(json.dumps(msg.serialize(), indent=2))

for operator, counts in operator_counts.items():
    print(f"{operator}:")

    for k, v in counts.items():
        print(f"{k}: {v}")

    print("")
