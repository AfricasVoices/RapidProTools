import json
import pytz
import argparse
from datetime import datetime

from dateutil.parser import isoparse
from temba_client.v2 import Message
from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger


log = Logger(__name__)
log.set_project_name("ComputeWindowOfDowntime")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute maximum window of time with 0 messages")
    parser.add_argument("input_file_path", metavar="input file", 
        help="File to read the raw data downloaded as json.",
    )
    parser.add_argument("output_file_path", metavar="output file",
        help="File to write the raw data downloaded as json.",
    )
    parser.add_argument("operator", metavar="operator",
        help="Operator that you'll need to analyze",
    )
    parser.add_argument("direction", metavar="direction of message",
        help="Direction of the message either sent or received by the operator",
    )
    parser.add_argument("start_date", metavar="start date", type=lambda s: isoparse(s),
        help="The start date as ISO 8601 string from which the window of downtime will be computed.",
    )
    parser.add_argument("end_date", metavar="end date", type=lambda s: isoparse(s),
        help="The end date as ISO 8601 string to which the window of downtime computation will end",
    )

    args = parser.parse_args()

    input_file_path = args.input_file_path
    output_file_path = args.output_file_path
    target_operator = args.operator 
    target_direction = args.direction
    start_date = args.start_date
    end_date = args.end_date

    with open(input_file_path, mode="r") as f:
        output = json.loads(f.read())
        messages = [Message.deserialize(val) for val in output]
        log.info("Loading messages from file {file_name}...")
        log.info(f"Loaded {len(messages)} messages")

    msg_sent_on_timestamps = []
    generated_outputs = []

    msg_sent_on_timestamps.insert(0, start_date)
    for msg in messages:
        if msg.urn.startswith("tel:"):
            operator = PhoneCleaner.clean_operator(msg.urn.split(":")[1])
        else:
            operator = msg.urn.split(":")[0]
        if operator == target_operator and msg.direction == target_direction:
            msg_sent_on_timestamps.append(msg.sent_on)
    msg_sent_on_timestamps.append(end_date)
    
    for index, time_in_range in enumerate(msg_sent_on_timestamps):
        log.debug(
            f"Computing window of time without messages {index + 1}/{len(msg_sent_on_timestamps)}..."
        )
        
        max_allowable_index = len(msg_sent_on_timestamps) - 1
        if index <  max_allowable_index:
            next_index = index + 1
        else:
            next_index = index 

        time_diff = msg_sent_on_timestamps[next_index] - msg_sent_on_timestamps[index]
        generated_outputs.append({
            "Operator": target_operator,
            "direction" : target_direction,
            "start" : str(msg_sent_on_timestamps[index]),
            "end" : str(msg_sent_on_timestamps[next_index]),
            "SecondsSinceLastMessage" : str(abs(time_diff.total_seconds()))
        })
             
    if output_file_path is not None:
        log.info(f"Logging {len(generated_outputs)} generated messages...")
        with open(output_file_path, mode="w") as f:
            json.dump(generated_outputs, f)
        log.info(f"Logged generated messages")
    else:
        log.debug("Not logging, the output file was not specified")
   
