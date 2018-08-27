import argparse
import os
import time

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable
from temba_client.v2 import TembaClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads runs from RapidPro.")
    parser.add_argument("--server", help="Address of RapidPro server. Defaults to http://localhost:8000.",
                        nargs="?", default="http://localhost:8000")
    parser.add_argument("--flow-name", help="Name of flow to filter on. If no name is provided, runs from all flows "
                                            "will be exported. ", nargs="?", default=None)
    parser.add_argument("token", help="RapidPro API Token")
    parser.add_argument("user", help="Identifier of user launching this program, for use in TracedData Metadata")
    parser.add_argument("mode", help="How to interpret downloaded runs. "
                                     "If 'all', outputs all runs from each contact. "
                                     "If 'latest-only', takes the latest value for each response field "
                                     "(while maintaining the history of older values in TracedData)",
                        choices=["all", "latest-only"])
    parser.add_argument("phone_uuid_table_path", metavar="phone-uuid-table-path",
                        help="JSON file containing an existing phone number <-> UUID lookup table. "
                             "This file will be updated with the new phone numbers which are found by this process")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to serialized TracedData JSON file")

    args = parser.parse_args()
    server = args.server
    flow_name = args.flow_name
    token = args.token
    user = args.user
    mode = args.mode
    phone_uuid_path = args.phone_uuid_table_path
    json_output_path = args.json_output_path

    rapid_pro = TembaClient(server, token)

    # Load the existing phone number <-> UUID table.
    if not os.path.exists(phone_uuid_path):
        raise FileNotFoundError("No such phone uuid table file '{}'. "
                                "To create a new, empty UUID table, "
                                "run $ echo \"{{}}\" > <target-json-file>".format(phone_uuid_path))
    with open(phone_uuid_path, "r") as f:
        phone_uuids = PhoneNumberUuidTable.load(f)

    # Determine id of flow to download
    print("Determining id for flow `{}`...".format(flow_name))
    if flow_name is None:
        flow_id = None
    else:
        flows = rapid_pro.get_flows().all(retry_on_rate_exceed=True)
        matching_flows = [f for f in flows if f.name == flow_name]

        if len(matching_flows) == 0:
            raise KeyError("Requested flow not found on RapidPro (Available flows: {})".format(
                           ",".join(list(map(lambda f: f.name, flows)))))
        if len(matching_flows) > 1:
            raise KeyError("Non-unique flow name")

        flow_id = matching_flows[0].uuid

    # Download all runs for the requested flow.
    print("Fetching runs...")
    start = time.time()
    runs = rapid_pro.get_runs(flow=flow_id).all(retry_on_rate_exceed=True)
    # IMPORTANT: The .all() approach may not scale to flows with some as yet unquantified "large" number of runs.
    # See http://rapidpro-python.readthedocs.io/en/latest/#fetching-objects for more details.
    print("Fetched {} runs ({}s)".format(len(runs), time.time() - start))

    # TODO: Check if the next two steps match current AVF practice.
    # Ignore flows which are incomplete because the respondent is still working through the questions.
    # runs = filter(lambda run: run.exited_on is not None, runs)
    # Ignore flows which are incomplete because the respondent stopped answering.
    # runs = filter(lambda run: run.exit_type == "completed", runs)
    # TODO: Ignore AVF test runs once testing is complete.

    # Sort by ascending order of modification date.
    runs = list(runs)
    runs.reverse()

    # Download all contacts into a dict of contact uuid -> contact.
    print("Fetching contacts...")
    start = time.time()
    contact_runs = {c.uuid: c for c in rapid_pro.get_contacts().all(retry_on_rate_exceed=True)}
    assert len(set(contact_runs.keys())) == len(contact_runs), "Non-unique contact UUID in RapidPro"
    print("Fetched {} contacts ({}s)".format(len(contact_runs), time.time() - start))

    # Convert the RapidPro run objects to TracedData.
    traced_runs = []
    for run in runs:
        contact_urns = contact_runs[run.contact.uuid].urns
        # assert len(contact_urns) == 1, "Contact has multiple URNs" TODO: Re-enable once AVF test runs are ignored.
        run_dict = {"avf_phone_id": phone_uuids.add_phone(contact_urns[0])}

        for category, response in run.values.items():
            run_dict[category.title() + " (Category) - " + run.flow.name] = response.category
            run_dict[category.title() + " (Value) - " + run.flow.name] = response.value
            # Convert from "input" to "text" here to match terminology in RP's Excel exports.
            run_dict[category.title() + " (Text) - " + run.flow.name] = response.input
            run_dict[category.title() + " (Name) - " + run.flow.name] = response.name
            run_dict[category.title() + " (Time) - " + run.flow.name] = response.time.isoformat()
            run_dict[category.title() + " (Run ID) - " + run.flow.name] = run.id

        if mode == "all":
            run_dict["created_on"] = run.created_on.isoformat()
            run_dict["modified_on"] = run.modified_on.isoformat()
            run_dict["exited_on"] = None if run.exited_on is None else run.exited_on.isoformat()
            run_dict["exit_type"] = run.exit_type

        traced_runs.append(TracedData(run_dict, Metadata(user, Metadata.get_call_location(), time.time())))

    if mode == "latest-only":
        # Keep only the latest values for each node for each contact
        contact_runs = dict()  # of contact_uuid -> traced_run
        for run in traced_runs:
            contact = run["avf_phone_id"]
            if contact not in contact_runs:
                contact_runs[contact] = run
            else:
                contact_runs[contact].append_data(
                    dict(filter(lambda x: x[0] != "contact_uuid", run.items())),
                    Metadata(user, Metadata.get_call_location(), time.time())
                )
        traced_runs = contact_runs.values()

    # Write the UUIDs out to a file
    with open(phone_uuid_path, "w") as f:
        phone_uuids.dump(f)

    # Output TracedData to JSON.
    if os.path.dirname(json_output_path) is not "" and not os.path.exists(os.path.dirname(json_output_path)):
        os.makedirs(os.path.dirname(json_output_path))
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(traced_runs, f, pretty_print=True)
