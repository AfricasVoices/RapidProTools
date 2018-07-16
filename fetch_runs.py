import argparse
import os
import time

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from temba_client.v2 import TembaClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poll RapidPro for flow runs")
    parser.add_argument("--server", help="Address of RapidPro server. Defaults to http://localhost:8000.",
                        nargs="?", default="http://localhost:8000")
    parser.add_argument("--flow-name", help="Name of flow to filter on. Defaults to returning runs for all flows",
                        nargs="?", default=None)
    parser.add_argument("token", help="RapidPro API Token", nargs=1)
    parser.add_argument("user", help="User launching this program", nargs=1)
    parser.add_argument("output", help="Path to output file", nargs=1)

    args = parser.parse_args()
    server = args.server
    flow_name = args.flow_name
    token = args.token[0]
    user = args.user[0]
    output_path = args.output[0]

    rapid_pro = TembaClient(server, token)

    print("Fetching...")
    start = time.time()

    if flow_name is None:
        flow_id = None
    else:
        flows = rapid_pro.get_flows().all()
        matching_flows = [f for f in flows if f.name == flow_name]

        if len(matching_flows) == 0:
            raise KeyError("Requested flow not found on RapidPro (Available flows: {})".format(
                           ",".join(list(map(lambda f: f.name, flows)))))
        if len(matching_flows) > 1:
            raise KeyError("Non-unique flow name")

        flow_id = matching_flows[0].uuid

    # Download all flow runs which have been updated since the last poll.
    runs = rapid_pro.get_runs(flow=flow_id).all()
    # IMPORTANT: The .all() approach may not scale to flows with some as yet unquantified "large" number of runs.
    # See http://rapidpro-python.readthedocs.io/en/latest/#fetching-objects for more details.

    end = time.time()
    print("Fetched. Time taken: " + str(end - start))

    # TODO: Check if the next two steps match current AVF practice.
    # TODO: Check if we should de-duplicate responses from participants who answer the survey more than once.
    # Ignore flows which are incomplete because the respondent is still working through the questions.
    runs = filter(lambda run: run.exited_on is not None, runs)
    # Ignore flows which are incomplete because the respondent stopped answering.
    runs = filter(lambda run: run.exit_type == "completed", runs)

    # Sort by ascending order of modification date
    runs = list(runs)
    runs.reverse()

    print(str(len(runs)) + " runs will be output")

    def process_run(run):
        data = {"contact_uuid": run.contact.uuid, "run_id": run.id}

        for category, response in run.values.items():
            data[category.title() + " (Category) - " + run.flow.name] = response.category
            data[category.title() + " (Value) - " + run.flow.name] = response.value
            data[category.title() + " (Time) - " + run.flow.name] = response.time.isoformat()

        data["created_on"] = run.created_on.isoformat()
        data["modified_on"] = run.modified_on.isoformat()
        data["exited_on"] = run.exited_on.isoformat()
        data["exit_type"] = run.exit_type

        return TracedData(data, Metadata(user, Metadata.get_call_location(), time.time()))

    # Convert runs to TracedData.
    traced_runs = list(map(process_run, runs))

    # Output TracedData to JSON.
    if os.path.dirname(output_path) is not "" and not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    with open(output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(traced_runs, f, pretty_print=True)
