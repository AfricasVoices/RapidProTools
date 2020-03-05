import json
import argparse
from dateutil.parser import isoparse

from temba_client.v2 import Message
from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger


log = Logger(__name__)
log.set_project_name("ComputeWindowOfDowntime")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute maximum window of time with 0 messages")
    parser.add_argument("raw_messages_file_path", metavar="input-file",
                        help="File to read the raw messages data downloaded as JSON")
    parser.add_argument("window_of_downtimes_output_file_path", metavar="output-file",
                        help="File to write the raw data downloaded as json.")
    parser.add_argument("target_operator", metavar="operator",
                        help="Operator to analyze for downtime")
    parser.add_argument("target_message_direction", metavar="direction-of-message", choices=('in', 'out'),
                        help="Direction of messages to limit the search for downtime to")
    parser.add_argument("start_date", metavar="start-date", type=lambda s: isoparse(s),
                        help="The start date as ISO 8601 string from which the window of downtime will be computed")
    parser.add_argument("end_date", metavar="end-date", type=lambda s: isoparse(s),
                        help="The end date as ISO 8601 string to which the window of downtime computation will end")

    args = parser.parse_args()

    raw_messages_file_path = args.raw_messages_file_path
    window_of_downtimes_output_file_path = args.window_of_downtimes_output_file_path
    target_operator = args.target_operator
    target_message_direction = args.target_message_direction
    start_date = args.start_date
    end_date = args.end_date

    with open(raw_messages_file_path, mode="r") as f:
        log.info("Loading messages from {raw_messages_file_path}...")
        output = json.loads(f.read())
        messages = [Message.deserialize(val) for val in output]
        log.info(f"Loaded {len(messages)} messages")

    msg_sent_on_timestamps = []
    msg_sent_on_timestamps.append(start_date)
    # Append `sent_on` timestamps to `msg_sent_on_timestamps` list
    # based on the target operator and target direction of the message
    for msg in messages:
        if msg.urn.startswith("tel:"):
            operator = PhoneCleaner.clean_operator(msg.urn.split(":")[1])
        else:
            operator = msg.urn.split(":")[0]
        if operator == target_operator and msg.direction == target_message_direction:
            msg_sent_on_timestamps.append(msg.sent_on)
    msg_sent_on_timestamps.append(end_date)

    computed_windows_of_downtime = []
    # Compute the time difference between two consecutive messages i.e `PreviousMessageTimestamp` and
    # `NextMessageTimestamp` to get the window of time without a message and relate each time difference
    #  with the operator and the message direction.
    for index, time_in_range in enumerate(msg_sent_on_timestamps):
        log.debug(
            f"Computing window of time without messages {index + 1}/{len(msg_sent_on_timestamps)}..."
        )

        max_allowable_index = len(msg_sent_on_timestamps) - 1
        if index < max_allowable_index:
            next_index = index + 1
        else:
            continue

        time_diff = msg_sent_on_timestamps[next_index] - \
            msg_sent_on_timestamps[index]
        computed_windows_of_downtime.append({
            "Operator": target_operator,
            "MessageDirection": target_message_direction,
            "PreviousMessageTimestamp": str(msg_sent_on_timestamps[index]),
            "NextMessageTimeTimestamp": str(msg_sent_on_timestamps[next_index]),
            "DownTimeDurationSeconds": str(abs(time_diff.total_seconds()))
        })

    log.info(
        f"Logging {len(computed_windows_of_downtime)} generated messages...")
    with open(window_of_downtimes_output_file_path, mode="w") as f:
        json.dump(computed_windows_of_downtime, f)
    log.info(f"Logged generated messages")
