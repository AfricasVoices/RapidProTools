import json
import argparse
from dateutil.parser import isoparse
from datetime import datetime, timedelta

from core_data_modules.logging import Logger
from temba_client.v2 import Message


def date_time_range(start, end, delta):
    current = start
    intervals = []
    while current < end:
        intervals.append(current)
        current += delta
    return intervals


log = Logger(__name__)
log.set_project_name("ComputeMessagesPerPeriod")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute the number of messages in each interval between the given start and end dates")
    parser.add_argument("raw_messages_file_path", metavar="raw-messages-file-path",
                        help="File to read the seralized Rapid Pro message data from")
    parser.add_argument("computed_messages_per_period_file_path", metavar="computed-messages-per-period-file-path",
                        help="File to write the computed messages per period data downloaded as json")
    parser.add_argument("message_difference_file_path", metavar="message-difference-file-path",
                        help="File to write the messages difference between two periods data downloaded as json")
    parser.add_argument("target_operator", metavar="target-operator",
                        help="Operator to analyze for downtime")
    parser.add_argument("target_message_direction", metavar="direction-of-message", choices=('in', 'out'),
                        help="Direction of messages to limit the search for downtime to")
    parser.add_argument("start_date", metavar="start-date", type=lambda s: isoparse(s),
                        help="The start date as ISO 8601 string from which the number of messages will be computed")
    parser.add_argument("end_date", metavar="end-date", type=lambda s: isoparse(s),
                        help="The end date as ISO 8601 string to which the number of messages computation will end")
    parser.add_argument("time_frame", metavar="time-frame", type=lambda s: datetime.strptime(s, '%H:%M:%S'),
                        help="The time frame (HH:MM:SS) to generate dates in intervals between the start and end date")

    args = parser.parse_args()

    raw_messages_file_path = args.raw_messages_file_path
    computed_messages_per_period_file_path = args.computed_messages_per_period_file_path
    message_difference_file_path = args.message_difference_file_path
    target_operator = args.target_operator
    target_message_direction = args.target_message_direction
    start_date = args.start_date
    end_date = args.end_date
    time_frame = args.time_frame

    with open(raw_messages_file_path, mode="r") as f:
        log.info("Loading messages from {raw_messages_file_path}...")
        input = json.load(f)
        messages = [Message.deserialize(val) for val in input]
        log.info(f"Loaded {len(messages)} messages")

    # Filter messages based on the target operator and target direction of the message
    log.info(f"Filter messages based on operator {target_operator} and, "
             "message direction as {target_message_direction}")
    log.info(f"{len(messages)} messages before filtering")
    filtered_messages = []
    for msg in messages:
        if msg.urn.startswith("tel:"):
            operator = PhoneCleaner.clean_operator(msg.urn.split(":")[1])
        else:
            operator = msg.urn.split(":")[0]
        if operator == target_operator and msg.direction == target_message_direction:
            log.info(f"Filtering messages...")
            filtered_messages.append(msg)
    log.info(f"{len(filtered_messages)} messages after filtering")

    time_interval = timedelta(hours=time_frame.hour,
                              minutes=time_frame.minute, seconds=time_frame.second)

    date_time_bounds = date_time_range(start_date, end_date, time_interval)

    # Compute number of messages between two datetime bounds i.e `PreviousMessageTimestamp` and
    # `NextMessageTimestamp` to get number of mesages per period and relate each quantity
    #  with the operator and the message direction.
    messages_per_period = []
    for index in range(len(date_time_bounds) - 2):
        next_index = index + 1

        messages_this_period = 0
        for msg in filtered_messages:
            if date_time_bounds[index] <= msg.sent_on < date_time_bounds[next_index]:
                messages_this_period += 1

        messages_per_period.append({
            "Operator": target_operator,
            "MessageDirection": target_message_direction,
            "periodStart": str(date_time_bounds[index]),
            "periodEnd": str(date_time_bounds[next_index]),
            "NumberOfMessages": messages_this_period
        })

    message_difference_per_period = []
    for index in range(len(messages_per_period) - 2):
        next_index = index + 1
        message_difference_per_period.append({
            "periodStart": str(messages_per_period[index]["periodStart"]),
            "periodBetween": str(messages_per_period[index]["periodEnd"]),
            "periodEnd": str(messages_per_period[next_index]["periodEnd"]),
            "MessageDifference": abs(messages_per_period[next_index]["NumberOfMessages"] - messages_per_period[index]["NumberOfMessages"])
        })

    log.info(f"Logging {len(messages_per_period)} generated messages...")
    with open(computed_messages_per_period_file_path, mode="w") as f:
        json.dump(messages_per_period, f)
    with open(message_difference_file_path, mode="w") as f:
        json.dump(message_difference_per_period, f)
    log.info(f"Logged generated messages")
