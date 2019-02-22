import argparse

from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combines all the data from the same run into a single run object")
    parser.add_argument("user", help="User running this program, for use in the TracedData Metadata")
    parser.add_argument("run_id_key", help="Key in TracedData objects of the run id", metavar="run-id-key")
    parser.add_argument("input_file_path", help="File containing TracedData runs to coalesce",
                        metavar="input-file-path")
    parser.add_argument("output_file_path", help="File to write the coalesced TracedData runs to",
                        metavar="output-file-path")

    args = parser.parse_args()
    user = args.user
    run_id_key = args.run_id_key
    input_file_path = args.input_file_path
    output_file_path = args.output_file_path

    with open(input_file_path) as f:
        input_runs = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    output_runs = []
    runs_lut = dict()
    for run in input_runs:
        run_id = run[run_id_key]

        if run_id in runs_lut:
            runs_lut[run_id].append_data(
                dict(run.items()),  # (Note that this drops history, which is probably only ok for this experiment)
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            )
        else:
            runs_lut[run_id] = run
            output_runs.append(run)

    output_runs.sort(key=lambda td: td["modified_on"])

    with open(output_file_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(output_runs, f, pretty_print=True)
