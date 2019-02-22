import argparse

from core_data_modules.traced_data.io import TracedDataJsonIO

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compares all the runs in two lists and asserts that they are all "
                                                 "equal.")
    parser.add_argument("input_file_1_path", help="File to compare", metavar="input-file-2-path")
    parser.add_argument("input_file_2_path", help="File to compare", metavar="input-file-2-path")

    args = parser.parse_args()
    input_file_1_path = args.input_file_1_path
    input_file_2_path = args.input_file_2_path

    with open(input_file_1_path) as f:
        input_runs_1 = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    with open(input_file_2_path) as f:
        input_runs_2 = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    for x, y in zip(input_runs_1, input_runs_2):
        assert x.items() == y.items(), f"Files differ (run_id {x['run_id - csap_s02_demog']} in file 1" \
            f" vs run_id {y['run_id - csap_s02_demog']} in file 2)"

    print("Files equal")
