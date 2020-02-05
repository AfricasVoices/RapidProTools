# Compute maximum window of time with 0 messages
import json
import pytz
import iso8601
from datetime import datetime
from temba_client.v2 import Message
from core_data_modules.logging import Logger


def group(lst, n):
    """group([0,3,4,10,2,3], 2) => [(0,3), (4,10), (2,3)]
    
    Group a list into consecutive n-tuples. Incomplete tuples are
    discarded e.g.
    
    >>> group(range(10), 3)
    [(0, 1, 2), (3, 4, 5), (6, 7, 8)]
    """
    return zip(*[lst[i::n] for i in range(n)])


log = Logger(__name__)
log.set_project_name("WindowOfDowntime")

if __name__ == "__main__":
    with open("data.txt", mode="r") as f:
        file_content = f.readline()
        output = json.loads(file_content)
        f.close()

    data = [Message.deserialize(val) for val in output]
    period_with_msg = []
    generated_outputs = []
    generated_data = []
    output_keys = ["traffic", "direction", "start", "end", "delta"]
    for msg in data:
        traffic = msg.urn.split(":")[0]
        direction = msg.direction
        if traffic == "telegram" and direction == "in":
            period_with_msg.append(msg.sent_on)
        # print(period_with_msg)
    for index, time_in_range in enumerate(period_with_msg, start=0):
        log.debug(
            f"Computing window of time without messages {index + 1}/{len(period_with_msg)}..."
        )
        for msg in data:
            traffic = msg.urn.split(":")[0]
            direction = msg.direction

            if traffic == "telegram" and direction == "in":
                try:
                    if period_with_msg[index] == msg.sent_on:
                        time_diff = period_with_msg[index + 1] - period_with_msg[index]
                        generated_outputs.append(traffic)
                        generated_outputs.append(direction)
                        generated_outputs.append(str(period_with_msg[index]))
                        generated_outputs.append(str(abs(time_diff.total_seconds())))
                except IndexError:
                    pass
    # group the list into 5 items
    # confirm if the last time is a time delta
    generated_outputs = list(group(generated_outputs, 5))
    #  print(generated_outputs)
    for output in generated_outputs:
        data_dict = dict(zip(output_keys, list(output)))
        generated_data.append(data_dict)
    # print(generated_data)
    with open("downtime.txt", mode="w") as f:
        f.write(json.dumps(generated_data))
        f.close()
