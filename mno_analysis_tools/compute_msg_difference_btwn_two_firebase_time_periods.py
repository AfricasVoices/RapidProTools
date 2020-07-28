import json
import argparse
from dateutil.parser import isoparse
from datetime import datetime, timedelta

from core_data_modules.logging import Logger
from core_data_modules.cleaners import PhoneCleaner
from temba_client.v2 import Message


def date_time_range(start, end, delta):
    current = start
    intervals = []
    while current < end:
        intervals.append(current)
        current += delta
    return intervals


log = Logger(__name__)
log.set_project_name("ComputeMessagesBetweenTwoFirebaseTimePeriods")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute message difference between two firebase time periods `the time period for firebase is a constant number`")
    parser.add_argument("raw_messages_input_file_path", metavar="raw-messages-input-file-path",
                        help="File to read the serialized Rapid Pro message data from")
    parser.add_argument("messages_difference_per_two_firebase_time_period_output_file_path", metavar="message-difference-output-file-path",
                        help=" File to write the messages difference between two firebase time periods data downloaded as JSON")
    parser.add_argument("target_operator", metavar="target-operator",
                        help="Operator to compute message difference between two firebase time periods")
    parser.add_argument("target_message_direction", metavar="target-message-direction", choices=('in', 'out'),
                        help="Direction of messages to limit the search for downtime to")
    parser.add_argument("start_date", metavar="start-date", type=lambda s: isoparse(s),
                        help="The start date as ISO 8601 string from which the number of messages will be computed")
    parser.add_argument("end_date", metavar="end-date", type=lambda s: isoparse(s),
                        help="The end date as ISO 8601 string to which the number of messages computation will end")
    parser.add_argument("-t", "--time_frame", metavar="time-frame", type=lambda s: datetime.strptime(s, '%H:%M:%S'),
                        default="00:00:10", help="The time frame (HH:MM:SS) to generate dates in intervals between the start and end date")

    args = parser.parse_args()

    raw_messages_input_file_path = args.raw_messages_input_file_path
    messages_difference_per_two_firebase_time_period_output_file_path = args.messages_difference_per_two_firebase_time_period_output_file_path
    target_operator = args.target_operator
    target_message_direction = args.target_message_direction
    start_date = args.start_date
    end_date = args.end_date
    if args.time_frame:
        time_frame = args.time_frame

    with open(raw_messages_input_file_path, mode="r") as f:
        log.info(f"Loading messages from {raw_messages_input_file_path}...")
        input = json.load(f)
        messages = [Message.deserialize(val) for val in input]
        log.info(f"Loaded {len(messages)} messages")

    # Filter messages based on the target operator and target direction of the message
    log.info(f"Filtering messages based on {target_operator} and "
             f"message direction as '{target_message_direction}' from {len(messages)} total messages ")
    filtered_messages = []
    for msg in messages:
        if msg.urn.startswith("tel:"):
            operator = PhoneCleaner.clean_operator(msg.urn.split(":")[1])
        else:
            operator = msg.urn.split(":")[0]
        if operator == target_operator and msg.direction == target_message_direction:
            msg_direction = msg.direction
            filtered_messages.append(msg)
    log.info(f"returning {len(filtered_messages)} messages")
    filtered_messages_timestamps = [msg.sent_on for msg in filtered_messages]

    time_interval = timedelta(hours=time_frame.hour,
                              minutes=time_frame.minute, seconds=time_frame.second)

    date_time_bounds = date_time_range(start_date, end_date, time_interval)

    timestamps_with_bounds = filtered_messages_timestamps + date_time_bounds
    timestamps_with_bounds.sort()

    # Compute the number of messages between two firebase time bounds i.e `PreviousMessageTimestamp` and
    # `NextMessageTimestamp` to get number of mesages in each firebase period and relate 
    #  each quantity with the operator and the message direction.
    messages_per_two_firebase_time_period = []
    for index in range(len(date_time_bounds) - 1):
        next_index = index + 1

        period_start = date_time_bounds[index]
        period_end = date_time_bounds[next_index]

        start_index = timestamps_with_bounds.index(period_start) + 1
        end_index = timestamps_with_bounds.index(period_end)
        messages_this_period = len(timestamps_with_bounds[start_index:end_index])
        timestamps_with_bounds = timestamps_with_bounds[end_index:]

        messages_per_two_firebase_time_period.append({
            "Operator": operator,
            "MessageDirection": msg_direction,
            "PeriodStart": period_start.isoformat(),
            "PeriodEnd": period_end.isoformat(),
            "NumberOfMessages": messages_this_period
        })

    # Compute message difference between two firebase time periods
    message_difference_per_two_firebase_time_period = []
    for index in range(len(messages_per_two_firebase_time_period) - 1):
        next_index = index + 1
        message_difference_per_two_firebase_time_period.append({
            "Operator": operator,
            "MessageDirection": msg_direction,
            "PeriodStart": messages_per_two_firebase_time_period[index]["PeriodStart"],
            "PeriodBetween": messages_per_two_firebase_time_period[index]["PeriodEnd"],
            "PeriodEnd": messages_per_two_firebase_time_period[next_index]["PeriodEnd"],
            "MessageDifference": messages_per_two_firebase_time_period[next_index]["NumberOfMessages"] - messages_per_two_firebase_time_period[index]["NumberOfMessages"]
        })

    log.info(f"writing message_difference_per_period json file...")
    with open(messages_difference_per_two_firebase_time_period_output_file_path, mode="w") as f:
        json.dump(message_difference_per_two_firebase_time_period, f)
    log.info(f"Logged generated messages")
