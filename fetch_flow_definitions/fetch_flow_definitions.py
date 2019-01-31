import argparse
import json
import time

from temba_client.v2 import TembaClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads flow definitions from Rapid Pro and writes to a json file")
    parser.add_argument("server", help="Address of RapidPro server")
    parser.add_argument("token", help="RapidPro API Token")
    parser.add_argument("flow_names", help="Names of the flows to download the definitions of",
                        nargs="+", default=None, metavar="flow-names")
    parser.add_argument("output_file_path", metavar="output-file-path",
                        help="Path to a JSON file to write the downloaded definitions to")

    args = parser.parse_args()
    server = args.server
    flow_names = args.flow_names
    token = args.token
    output_file_path = args.output_file_path

    rapid_pro = TembaClient(server, token)

    flow_ids = []
    for flow_name in flow_names:
        print("Determining id for flow `{}`...".format(flow_name))
        flows = rapid_pro.get_flows().all(retry_on_rate_exceed=True)
        matching_flows = [f for f in flows if f.name == flow_name]

        if len(matching_flows) == 0:
            raise KeyError(f"Requested flow '{flow_name}' not found on RapidPro "
                           f"(Available flows: {','.join([f.name for f in flows])})")
        if len(matching_flows) > 1:
            raise KeyError(f"Non-unique flow name: '{flow_name}'")

        flow_id = matching_flows[0].uuid
        flow_ids.append(flow_id)

    # Download the flow definition(s) from Rapid Pro
    print(f"Fetching definitions for {len(flow_ids)} flow(s)...")
    start = time.time()
    definitions = rapid_pro.get_definitions(flows=flow_ids, dependencies="none")
    print(f"Fetched flow definitions ({time.time() - start}s)")

    with open(output_file_path, "w") as f:
        json.dump(definitions.serialize(), f, indent=2)
