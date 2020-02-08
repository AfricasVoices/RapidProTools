import argparse

from core_data_modules.logging import Logger
from rapid_pro_tools import RapidProClient

log = Logger(__name__)
log.set_project_name("FetchRawMessages")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetches archives from Rapid Pro instance")
    parser.add_argument("domain", metavar="domain",
        help="Domain that the instance of Rapid Pro is running on",
    )
    parser.add_argument("token", metavar="token",
        help="Token for authenticating to the instance",
    )
    parser.add_argument("output_file", metavar="file", type=argparse.FileType(mode="w"),
        help="File to write the raw data downloaded as json.",
    )

    args = parser.parse_args()
  
    source_domain = args.domain
    source_token = args.token
    output_file = args.output_file

    source_instance = RapidProClient(source_domain, source_token)

    log.info("fetch raw messages...")
    source_instance.get_raw_messages(raw_export_log_file=output_file)