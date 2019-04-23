import argparse
import datetime

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger
from dateutil.parser import isoparse

from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)

parser = argparse.ArgumentParser(description="Downloads a day of messages from a RapidPro server and prints the "
                                             "number of sent/received/failed messages for each operator")
parser.add_argument("server", help="Domain of the Rapid Pro server to download messages from")
parser.add_argument("token", help="API token for authenticating with the Rapid Pro server")
parser.add_argument("date", help="ISO 8601 formatted string containing the date of the messages to download "
                                 "e.g. '2019-05-20'")

args = parser.parse_args()
server = args.server
token = args.token
date = isoparse(args.date)

rapid_pro = RapidProClient(server, token)

start_date = date
end_date = start_date + datetime.timedelta(days=1)
log.info(f"Fetching messages created between {start_date.isoformat()} and {end_date.isoformat()}...")
raw_messages = rapid_pro \
    .get_raw_messages(created_after_inclusive=start_date, created_before_exclusive=end_date)\
    .all(retry_on_rate_exceed=True)
log.info(f"Fetched {len(raw_messages)} messages created between {start_date.isoformat()} and {end_date.isoformat()}")

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
            log.warning(f"Unexpected message status '{msg.status}'")
    else:
        log.warning(f"Unexpected message direction '{msg.direction}'")

log.info("Outputting counts for each operator...")
for operator, counts in operator_counts.items():
    print(f"{operator}:")

    for k, v in counts.items():
        print(f"{k}: {v}")

    print("")
