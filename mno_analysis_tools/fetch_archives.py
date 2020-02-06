import json
import argparse

from core_data_modules.logging import Logger
from rapid_pro_tools import RapidProClient
from temba_client.v2 import Message

log = Logger(__name__)
log.set_project_name("FetchArchives")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetches archives from Rapid Pro instance"
    )

    parser.add_argument(
        "domain",
        metavar="domain",
        help="Domain that the instance of Rapid Pro is running on",
    )
    parser.add_argument(
        "token",
        metavar="token",
        help="token for authenticating to the instance",
    )

    args = parser.parse_args()

    source_domain = args.domain
    source_token = args.token
    source_instance = RapidProClient(source_domain, source_token)

    available_archives_list = source_instance.list_archives("message")
    log.info(f"Fetched {len(available_archives_list)} archives")

    list_of_message_objects = []
    for archive_metadata in available_archives_list:
        list_of_message_objects.append(source_instance.get_archive(archive_metadata))

    output = [
        Message.serialize(attribute)
        for message_object in list_of_message_objects
        for attribute in message_object
    ]

    with open("data.txt", mode="w") as f:
        f.write(json.dumps(output))
