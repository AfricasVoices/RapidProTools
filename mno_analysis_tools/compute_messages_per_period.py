import json
import pytz
import iso8601
import argparse
from datetime import datetime, timedelta

from core_data_modules.logging import Logger
from temba_client.v2 import Message


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


log = Logger(__name__)
log.set_project_name("ComputeNumberofMessages")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Number of messages between two firebase time periods ")
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
        help="The start date (yyyy-mm-dd) from which the datetime range will start.",
    )
    parser.add_argument("end_date", metavar="end date",
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').astimezone(pytz.UTC),
        help="The end date (yyy-mm-dd) to which the datetime range will end",
    )
    parser.add_argument("delta", metavar="time delta",
        type=lambda s: datetime.strptime(s, '%H:%M:%S'),
        help="The delta (HH:MM:SS) to generate the time range",
    )

    args = parser.parse_args()

    input_file = args.input_file
    output_file = args.output_file
    operator = args.communication_medium
    msg_direction = args.direction
    start_date = args.start_date
    end_date = args.end_date
    date_time = args.delta

    with input_file as file:
        file_content = file.readline()
        output = json.loads(file_content)
        messages = [Message.deserialize(val) for val in output]
        log.info(f"Read {len(messages)} messages...")

    filtered_messages = []
    for msg in messages:
        communication_medium = msg.urn.split(":")[0]
        direction = msg.direction
        if communication_medium == operator and direction == msg_direction:
            filtered_messages.append(msg)

    delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)

    time_range = [dt for dt in datetime_range(start_date, end_date, delta)]

    generated_outputs = []
    for index, time_in_range in enumerate(time_range):
        msg_no = 0

        max_allowable_index = len(time_range) - 1
        if index <  max_allowable_index:
            next_index = index + 1
        else:
            next_index = index 

        for msg in filtered_messages:
            communication_medium = msg.urn.split(":")[0]
            direction = msg.direction
            
            if time_range[index] <= msg.sent_on < time_range[next_index]:
                msg_no += 1
        
        generated_outputs.append({
            "communication_medium" : communication_medium,
            "direction" : direction,
            "start" : str(time_range[index]),
            "end" : str(time_range[next_index]),
            "messages" : msg_no
        })
    
    if output_file is not None:
        log.info(f"Logging {len(generated_outputs)} generated messages...")
        json.dump(generated_outputs, output_file)
        output_file.write("\n")
        log.info(f"Logged generated messages")
    else:
        log.debug("Not logging, the output file was not specified")



