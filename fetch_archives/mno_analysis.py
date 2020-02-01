import json
import pytz
import iso8601
from datetime import datetime, timedelta
from temba_client.v2 import Message

with open("data.txt", mode="r") as f:
    file_content = f.readline()
    output = json.loads(file_content)
    data = [Message.deserialize(val) for val in output]


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
    return (dt - epoch).total_seconds() * 1000.0


def group(lst, n):
    """group([0,3,4,10,2,3], 2) => [(0,3), (4,10), (2,3)]
    
    Group a list into consecutive n-tuples. Incomplete tuples are
    discarded e.g.
    
    >>> group(range(10), 3)
    [(0, 1, 2), (3, 4, 5), (6, 7, 8)]
    """
    return zip(*[lst[i::n] for i in range(n)])


# Compute maximum window of time with 0 messages
def window_of_downtime():
    timelist = []
    list1 = []
    for item in data:
        timelist.append(item.sent_on)
    dts = [unix_time_millis(dt) for dt in timelist]
    for index, time_in_range in enumerate(dts, start=0):
        try:
            time_diff = timelist[index + 1] - timelist[index]
            list1.append(dts[index])
            list1.append(dts[index + 1])
            list1.append(time_diff)
        except IndexError:
            pass
    # group the list into 3 items
    list1 = list(group(list1, 3))
    print(list1)
    # confirm if the last time is a time delta


def no_msg():
    start = iso8601.parse_date("2019-07-03T10:00:00Z")
    end = iso8601.parse_date("2019-10-03T10:07:00Z")

    dts = [
        dt
        for dt in datetime_range(unix_time_millis(start), unix_time_millis(end), 2000.0)
    ]

    for index, time_in_range in enumerate(dts, start=0):
        msg_no = 0
        list2 = []
        for msg in data:
            date_time2 = unix_time_millis(msg.sent_on)
            try:
                if dts[index] <= date_time2 < dts[index + 1]:
                    msg_no += 1
                    list2.append(dts[index])
                    list2.append(dts[index + 1])
                    list2.append(msg_no)
                else:
                    list2.append(dts[index])
                    list2.append(dts[index + 1])
                    list2.append(msg_no)
            except IndexError:
                pass
        if len(list2) > 3:
            list2 = list2[-3:]
        if list2[-1] != 0 and type(list2[-1]) != float:
            print(list2)


# no_msg()
# window_of_downtime()

# Messages received per time over the period of the project.
# Add Total incoming or Outgoing, Operator to the generated output => Total incoming + Outgoing / Operator
# Write function to generate data when either of the two functions run
# Draw the graphs
