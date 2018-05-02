import argparse
import time
import datetime

import pytz
from temba_client.v2 import TembaClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poll RapidPro for flow runs")
    parser.add_argument("token", help="RapidPro API Token", nargs=1)
    parser.add_argument("--server", help="Address of RapidPro server. Defaults to localhost:8000.",
                        nargs="?", default="http://localhost:8000/")
    parser.add_argument("--poll_interval", help="Time to wait between polling the server, in seconds. Defaults to 5.",
                        nargs="?", default=5, type=int)

    args = parser.parse_args()
    token = args.token[0]
    server = args.server
    poll_interval = args.poll_interval

    client = TembaClient(server, token)
    last_update_time = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, pytz.utc)

    while True:
        time.sleep(poll_interval)

        print("Polling")
        start = time.time()

        # Download all flows which have been updated since the last poll.
        flows = client.get_runs(after=last_update_time).all(retry_on_rate_exceed=True)
        # IMPORTANT: The .all() approach may not scale to flows with some as yet unquantified "large" number of runs.
        # See http://rapidpro-python.readthedocs.io/en/latest/#fetching-objects for more details.

        end = time.time()
        print("Polled")
        print("Time taken: " + str(end - start))

        # Ignore flows which are incomplete because the respondent is still working through the questions.
        flows = filter(lambda flow: flow.exited_on is not None, flows)

        # Ignore flows which are incomplete because the respondent stopped answering.
        flows = filter(lambda flow: flow.exit_type == "completed", flows)

        # Sort by ascending order of modification date
        flows.reverse()

        print("Fetched " + str(len(flows)) + " flows")

        if len(flows) == 0:
            continue

        # Print some data about the flow:
        for flow in flows:
            print("Contact: " + flow.contact.uuid)
            print("Update Time: " + str(flow.modified_on))
            for question, answer in flow.values.iteritems():
                print("  " + question)
                print("  " + "  " + answer.value)
            print("")

        last_update_time = flows[-1].modified_on + datetime.timedelta(microseconds=1)  # TODO: Check if using time is ok

