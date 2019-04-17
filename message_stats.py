import argparse
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
    .get_raw_messages(last_modified_after_inclusive=date)\
    .all(retry_on_rate_exceed=True)
log.info(f"Fetched {len(raw_messages)} raw messages last modified since {date.isoformat()}")

# Filter for incoming messages only
incoming_messages = [msg for msg in raw_messages if msg.direction == "in"]
log.info(f"Filtered for incoming messages only: {len(incoming_messages)}/{len(raw_messages)} messages remain.")

# Filter for messages actually received today
incoming_messages_today = [msg for msg in incoming_messages if msg.sent_on > date]
log.info(f"Filtered for messages actually sent since {date.isoformat()}: "
         f"{len(incoming_messages_today)}/{len(incoming_messages)} messages remain.")

# Filter for messages received from numbers
incoming_phone_messages_today = [msg for msg in incoming_messages_today if msg.urn.startswith("tel:")]
log.info(f"Filtered for messages sent from a telephone: "
         f"{len(incoming_phone_messages_today)}/{len(incoming_messages_today)} messages remain.")

# Compute number of messages received by each operator
operator_counts = dict()  # of operator -> count
for msg in incoming_phone_messages_today:
    phone_number = msg.urn.replace("tel:", "")
    operator = PhoneCleaner.clean_operator(phone_number)
    if operator not in operator_counts:
        operator_counts[operator] = 0
    operator_counts[operator] += 1

for operator, count in operator_counts.items():
    print(f"{operator}: {count}")


# for msg in raw_messages:
#     if msg.direction == "in":
#         print(json.dumps(msg.serialize(), indent=2))

# sample_message = messages_today[0]
# print(json.dumps(sample_message.serialize(), indent=2))


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
