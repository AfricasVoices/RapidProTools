import json
import argparse
from dateutil.parser import isoparse
from datetime import datetime, timedelta

from core_data_modules.logging import Logger
from temba_client.v2 import Message


def date_time_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


log = Logger(__name__)
log.set_project_name("ComputeNumberofMessages")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Number of messages between two firebase time periods ")
    parser.add_argument("input_file_path", metavar="input-file",
                        help="File to read the raw data downloaded as json",
                        )
    parser.add_argument("output_file_path", metavar="output-file",
                        help="File to write the raw data downloaded as json",
                        )
    parser.add_argument("operator", metavar="operator",
                        help="Operator to analyze for downtime",
                        )
    parser.add_argument("message_direction", metavar="direction-of-message", choices=('in', 'out'),
                        help="Direction of messages to limit the search for downtime to",
                        )
    parser.add_argument("start_date", metavar="start date", type=lambda s: isoparse(s),
                        help="The start date as ISO 8601 string from which the number of messages will be computed",
                        )
    parser.add_argument("end_date", metavar="end date", type=lambda s: isoparse(s),
                        help="The end date as ISO 8601 string to which the number of messages computation will end",
                        )
    parser.add_argument("time_frame", metavar="time-frame", type=lambda s: datetime.strptime(s, '%H:%M:%S'),
                        help="The time frame (HH:MM:SS) to generate dates in intervals between the start and end date",
                        )

    args = parser.parse_args()

    input_file_path = args.input_file_path
    output_file_path = args.output_file_path
    target_operator = args.operator
    target_direction = args.message_direction
    start_date = args.start_date
    end_date = args.end_date
    time_frame = args.time_frame

    with open(input_file_path, mode="r") as f:
        log.info("Loading messages from {input_file_path}...")
        output = json.loads(f.read())
        messages = [Message.deserialize(val) for val in output]
        log.info(f"Loaded {len(messages)} messages")

    filtered_messages = []
    for msg in messages:
        if msg.urn.startswith("tel:"):
            operator = PhoneCleaner.clean_operator(msg.urn.split(":")[1])
        else:
            operator = msg.urn.split(":")[0]
        if operator == target_operator and msg.direction == target_direction:
            filtered_messages.append(msg)

    time_interval = timedelta(hours=time_frame.hour,
                              minutes=time_frame.minute, seconds=time_frame.second)

    date_time_bounds = [date_time for date_time in date_time_range(
        start_date,  end_date, time_interval)]

    computed_number_of_messages = []
    for index, date_time_bound in enumerate(date_time_bounds):
        number_of_messages = 0

        max_allowable_index = len(date_time_bounds) - 1
        if index < max_allowable_index:
            next_index = index + 1
        else:
            continue

        for msg in filtered_messages:
            if date_time_bounds[index] <= msg.sent_on < date_time_bounds[next_index]:
                number_of_messages += 1

        computed_number_of_messages.append({
            "Operator": target_operator,
            "MessageDirection": target_direction,
            "PreviousMessageTimestamp": str(date_time_bounds[index]),
            "NextMessageTimestamp": str(date_time_bounds[next_index]),
            "NumberOfMessages": msg_number
        })

        log.info(
            f"Logging {len(computed_number_of_messages)} generated messages...")
        with open(output_file_path, mode="w") as f:
            json.dump(computed_number_of_messages, f)
        log.info(f"Logged generated messages")
