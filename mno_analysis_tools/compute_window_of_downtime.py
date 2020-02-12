import json
import pytz
import argparse
from datetime import datetime

from temba_client.v2 import Message
from core_data_modules.logging import Logger
from dateutil.parser import isoparse


log = Logger(__name__)
log.set_project_name("ComputeWindowOfDowntime")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute maximum window of time with 0 messages")
    parser.add_argument("input_file", metavar="input file", 
        help="File to read the raw data downloaded as json.",
    )
    parser.add_argument("output_file", metavar="output file",
        help="File to write the raw data downloaded as json.",
    )
    parser.add_argument("communication_medium", metavar="operator",
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
    print(type(args.start_date))

    input_file = args.input_file
    output_file = args.output_file
    operator = args.communication_medium
    msg_direction = args.direction
    start_date = args.start_date
    end_date = args.end_date

    with open(input_file) as f:
        output = json.loads(f.read())
        messages = [Message.deserialize(val) for val in output]
        log.info("Loading messages from file {file_name}...")
        log.info(f"Loaded {len(messages)} messages")

    period_with_msg = []
    generated_outputs = []

    period_with_msg.insert(0, start_date)
    for msg in messages:
        communication_medium = msg.urn.split(":")[0]
        direction = msg.direction
        if communication_medium == operator and direction == msg_direction:
            period_with_msg.append(msg.sent_on)
    period_with_msg.append(end_date)
    
    for index, time_in_range in enumerate(period_with_msg):
        log.debug(
            f"Computing window of time without messages {index + 1}/{len(period_with_msg)}..."
        )
        
        max_allowable_index = len(period_with_msg) - 1
        if index <  max_allowable_index:
            next_index = index + 1
        else:
            next_index = index 

        time_diff = period_with_msg[next_index] - period_with_msg[index]
        generated_outputs.append({
            "communication_medium" : operator,
            "direction" : msg_direction,
            "start" : str(period_with_msg[index]),
            "end" : str(period_with_msg[next_index]),
            "delta" : str(abs(time_diff.total_seconds()))
        })
             
    if output_file is not None:
        log.info(f"Logging {len(generated_outputs)} generated messages...")
        with open(output_file, mode="w") as f:
            json.dump(generated_outputs, f)
        log.info(f"Logged generated messages")
    else:
        log.debug("Not logging, the output file was not specified")
   
