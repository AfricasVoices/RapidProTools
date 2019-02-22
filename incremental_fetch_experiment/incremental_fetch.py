import argparse
import datetime
import subprocess

from core_data_modules.traced_data.io import TracedDataJsonIO
from dateutil.parser import isoparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads runs from RapidPro and exports to TracedData. "
                                                 "If a previous export is detected in the data-dir, only retrieves "
                                                 "runs which were last modified since the last fetch.")
    parser.add_argument("user", help="User running this program, for use in the TracedData Metadata")
    parser.add_argument("server", help="Address of RapidPro server. e.g. https://app.rapidpro.io")
    parser.add_argument("token", help="RapidPro API Token")
    parser.add_argument("flow_name", help="Name of flow to filter on. If no name is provided, runs from all flows "
                                          "will be exported.", metavar="flow-name")
    parser.add_argument("data_dir", help="Directory to store exported data files in", metavar="data-dir")

    args = parser.parse_args()
    user = args.user
    server = args.server
    token = args.token
    flow_name = args.flow_name
    data_dir = args.data_dir

    # Download all the runs for each of the radio shows
    output_file_path = f"{data_dir}/all_batched.json"
    uuid_table_path = f"{data_dir}/uuids.json"

    last_modified = isoparse("2000-01-01T00:00:00+00:00")
    data = []
    try:
        with open(output_file_path) as f:
            data = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
            for td in data:
                if isoparse(td["modified_on"]) > last_modified:
                    last_modified = isoparse(td["modified_on"])
    except:
        pass

    last_modified += datetime.timedelta(microseconds=1)

    print(f"Exporting flow '{flow_name}' to '{output_file_path}'...")
    subprocess.run([
        "./docker-run.sh",
        "--flow-name", flow_name,
        "--range-start-inclusive", last_modified.isoformat(),
        server,
        token,
        user,
        "all",
        uuid_table_path,
        f"{data_dir}/batch_{last_modified.isoformat()}.json"
    ], cwd="../fetch_runs", check=True)

    with open(f"{data_dir}/batch_{last_modified.isoformat()}.json") as f:
        data.extend(list(TracedDataJsonIO.import_json_to_traced_data_iterable(f)))

    with open(output_file_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)
