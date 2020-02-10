import json
import pytz
import argparse
from datetime import datetime

from temba_client.v2 import Message
from core_data_modules.logging import Logger


log = Logger(__name__)
log.set_project_name("ComputeWindowOfDowntime")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute maximum window of time with 0 messages")
    parser.add_argument("input_file", metavar="input file", type=argparse.FileType(mode="r"),
        help="File to read the raw data downloaded as json.",
    )
    parser.add_argument("output_file", metavar="output file", type=argparse.FileType(mode="w"),
        help="File to write the raw data downloaded as json.",
    )
    parser.add_argument("communication_medium", metavar="operator",
        help="Operator that you'll need to analyze",
    )
    parser.add_argument("direction", metavar="direction of message",
        help="Direction of the message either sent or received by the operator",
    )
    parser.add_argument("start_date", metavar="start date", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').astimezone(pytz.UTC),
        help="The start date (yyyy-mm-dd) from which the window of downtime will be computed.",
    )
    parser.add_argument("end_date", metavar="end date",
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').astimezone(pytz.UTC),
        help="The end date (yyy-mm-dd) to which the window of downtime computation will end",
    )

    args = parser.parse_args()

    input_file = args.input_file
    output_file = args.output_file
    operator = args.communication_medium
    msg_direction = args.direction
    start_date = args.start_date
    end_date = args.end_date

    with input_file as file:
        file_content = file.readline()
        output = json.loads(file_content)
        messages = [Message.deserialize(val) for val in output]
        log.info(f"Read {len(messages)} messages...")

    period_with_msg = []
    generated_outputs = []

    period_with_msg.insert(0, start_date)
    for msg in messages:
        communication_medium = msg.urn.split(":")[0]
        direction = msg.direction
        if communication_medium == operator and direction == msg_direction:
            period_with_msg.append(msg.sent_on)
    period_with_msg.insert(len(period_with_msg), end_date)
    
    for index, time_in_range in enumerate(period_with_msg):
        log.debug(
            f"Computing window of time without messages {index + 1}/{len(period_with_msg)}..."
        )
        max_allowable_index = len(period_with_msg) - 1
        if (index + 1) <=  max_allowable_index:
            time_diff = period_with_msg[index + 1] - period_with_msg[index]
            end = str(period_with_msg[index + 1])
        else:
            time_diff = period_with_msg[-1] - period_with_msg[index]
            end = str(period_with_msg[-1])

        generated_outputs.append({
            "communication_medium" : communication_medium,
            "direction" : direction,
            "start" : str(period_with_msg[index]),
            "end" : end,
            "delta" : str(abs(time_diff.total_seconds()))
        })
             
    if output_file is not None:
        log.info(f"Logging {len(generated_outputs)} generated messages...")
        json.dump(generated_outputs, output_file)
        output_file.write("\n")
        log.info(f"Logged generated messages")
    else:
        log.debug("Not logging, the output file was not specified")
   
