import json
import pytz
import iso8601
from datetime import datetime

from core_data_modules.logging import Logger
from temba_client.v2 import Message


def group(lst, n):
    """group([0,3,4,10,2,3], 2) => [(0,3), (4,10), (2,3)]
    
    Group a list into consecutive n-tuples. Incomplete tuples are
    discarded e.g.
    
    >>> group(range(10), 3)
    [(0, 1, 2), (3, 4, 5), (6, 7, 8)]
    """
    return zip(*[lst[i::n] for i in range(n)])


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta



def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
    return (dt - epoch).total_seconds() * 1000.0


log = Logger(__name__)
log.set_project_name("WindowOfDowntime")

if __name__ == "__main__":
    with open("data.txt", mode="r") as f:
        file_content = f.readline()
        output = json.loads(file_content)
        data = [Message.deserialize(val) for val in output]

    start = iso8601.parse_date("2019-07-29T10:00:00Z")
    end = iso8601.parse_date("2019-9-04T10:00:00Z")

    dts = [
        dt
        for dt in datetime_range(unix_time_millis(start), unix_time_millis(end), 5000.0)
    ]

    results = []
    for index, time_in_range in enumerate(dts, start=0):
        msg_no = 0
        list2 = []
        for msg in data:
            date_time2 = unix_time_millis(msg.sent_on)
            traffic = msg.urn.split(":")[0]
            direction = msg.direction
            keys = ["traffic", "direction", "start", "end", "messages"]
            try:
                if dts[index] <= date_time2 < dts[index + 1]:
                    msg_no += 1
                    list2.append(traffic)
                    list2.append(direction)
                    list2.append(dts[index])
                    list2.append(dts[index + 1])
                    list2.append(msg_no)
                else:
                    list2.append(traffic)
                    list2.append(direction)
                    list2.append(dts[index])
                    list2.append(dts[index + 1])
                    list2.append(msg_no)
            except IndexError:
                pass
        if len(list2) > 3:
            list2 = list2[-5:]
        # Test to establish if the available records have been classified
        if list2[-1] != 0 and type(list2[-1]) != float:
            print(list2)
        if type(list2[-1]) != float:
            out = dict(zip(keys, list2))
        results.append(out)
    # print(results)
    with open("results.txt", mode="w") as f:
        f.write(json.dumps(results))



