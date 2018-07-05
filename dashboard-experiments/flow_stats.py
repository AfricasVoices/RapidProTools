import argparse
import time

from temba_client.v2 import TembaClient

start_time = 0


def start_timer():
    global start_time
    start_time = time.time()


def end_timer():
    print("Done [{:.2f}s]".format(time.time() - start_time))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uses the flows, runs, and contacts API to derive some stats "
                                                 "about the the specified flow")
    parser.add_argument("server", help="Base URL of server channel to connect to, including c/ex/<uuid>/", nargs=1)
    parser.add_argument("api_token", metavar="api-token", help="Rapid Pro API Token", nargs=1)
    parser.add_argument("flow", help="Name of flow to enquire about.", nargs=1)

    args = parser.parse_args()
    server = args.server[0]
    api_token = args.api_token[0]
    flow_name = args.flow[0]

    rapid_pro = TembaClient(server, api_token)

    # Get the id for the given flow name
    print("Downloading flows...")
    start_timer()
    flows = rapid_pro.get_flows().all()
    end_timer()
    matching_flows = [f for f in flows if f.name == flow_name]

    if len(matching_flows) == 0:
        exit("No matching flows")
    if len(matching_flows) > 1:
        exit("Too many matches")
    flow_id = matching_flows[0].uuid

    # Download flow stats.
    flow = rapid_pro.get_flows(uuid=flow_id).first()

    # Print stats
    print("Overview stats for flow '{}':".format(flow_name))
    print("Active: {}".format(flow.runs.active))
    print("Completed: {}".format(flow.runs.completed))
    print("Interrupted: {}".format(flow.runs.interrupted))
    print("Expired: {}".format(flow.runs.expired))  # (Duration until expiry available in flow.expires)

    print("")

    # Download run data.
    start_timer()
    print("Downloading runs...")
    runs = rapid_pro.get_runs(flow=flow_id).all()
    end_timer()

    # Print unique respondents
    unique_respondents = set()
    for run in runs:
        unique_respondents.add(run.contact.uuid)

    print("Total flow starts: {}".format(
        flow.runs.active + flow.runs.completed + flow.runs.interrupted + flow.runs.expired))
    print("Unique flow respondents: {}".format(len(unique_respondents)))

    print("")

    # Print number who reached each flow stage
    print("Number of times each flow stage was reached:")
    responses = dict()
    for run in runs:
        for category, response in run.values.items():
            if category.title() not in responses:
                responses[category.title()] = 0
            responses[category.title()] += 1

    for k, v in responses.items():
        print("{}: {}".format(k, v))

    # Contacts
    print("\nDownloading contacts...")
    contacts = rapid_pro.get_contacts().all()
    end_timer()

    print("")

    # Print number of respondents with each demographic field
    field_counts = dict()
    for contact in contacts:
        for k, v in contact.fields.items():
            if v is None:
                continue

            if k not in field_counts:
                field_counts[k] = 0
            field_counts[k] += 1

    print("Number of contacts with each demographic field (contacts API):")
    for k, v in field_counts.items():
        print("{}: {}".format(k, v))

    print("")

    # Re-construct contact demographic info from runs
    contacts = dict()
    runs.sort(key=lambda r: r.modified_on)
    for run in runs:
        if run.contact.uuid not in contacts:
            contacts[run.contact.uuid] = dict()

        contact = contacts[run.contact.uuid]

        for category, response in run.values.items():
            contact[category.title()] = response.value

    # Again, print the number of respondents who answered each field, but using the re-constructed
    # contact data this time.
    print("Number of contacts with each run demographic field (runs API):")
    field_counts = dict()
    for contact in contacts.values():
        for k, v in contact.items():
            if v is None:
                continue

            if k not in field_counts:
                field_counts[k] = 0
            field_counts[k] += 1

    for k, v in field_counts.items():
        print("{}: {}".format(k, v))
