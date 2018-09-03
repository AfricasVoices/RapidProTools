import argparse
import json
import os
import time

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable, IOUtils
from temba_client.v2 import TembaClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads contacts from RapidPro")
    parser.add_argument("--server", help="Address of RapidPro server. Defaults to http://localhost:8000.",
                        nargs="?", default="http://localhost:8000")
    parser.add_argument("--test-contacts-path",
                        help="Path to a JSON file containing a list of Rapid Pro contact UUIDs. "
                             "Messages sent from one of this ids will have the key \"test_run\" set to True "
                             "in the output JSON", nargs="?", default=None)
    parser.add_argument("token", help="RapidPro API Token")
    parser.add_argument("user", help="Identifier of user launching this program, for use in TracedData Metadata")
    parser.add_argument("phone_uuid_table_path", metavar="phone-uuid-table-path",
                        help="JSON file containing an existing phone number <-> UUID lookup table. "
                             "This file will be updated with the new phone numbers which are found by this process")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to serialized TracedData JSON file")

    args = parser.parse_args()
    server = args.server
    token = args.token
    test_contacts_path = args.test_contacts_path
    user = args.user
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

    # Load test contacts path if set, otherwise default to empty set
    test_contacts = set()
    if test_contacts_path is not None:
        with open(test_contacts_path, "r") as f:
            test_contacts.update(json.load(f))

    # Download all contacts
    print("Fetching contacts...")
    start = time.time()
    contacts = rapid_pro.get_contacts().all(retry_on_rate_exceed=True)
    print("Fetched {} contacts ({}s)".format(len(contacts), time.time() - start))

    # Filter out contacts with no contact information
    contacts = [contact for contact in contacts if len(contact.urns) > 0]

    # Convert contacts to TracedData
    traced_contacts = []
    for contact in contacts:
        print("{}/{}".format(len(traced_contacts) + 1, len(contacts)))
        contact_dict = dict()
        contact_dict["avf_phone_id"] = phone_uuids.add_phone(contact.urns[0])

        if contact.uuid in test_contacts:
            contact_dict["test_run"] = True

        # local_keys = ["District", "District Review", "Distirct", "Distirct Review"]
        # contact_dict.update({key: contact.field[key] for key in local_keys})

        contact_dict.update(contact.fields)

        traced_contacts.append(TracedData(
            contact_dict,
            Metadata(user, Metadata.get_call_location(), time.time())
        ))

    contacts = None

    # Write the UUIDs out to a file
    with open(phone_uuid_path, "w") as f:
        phone_uuids.dump(f)

    # Output TracedData to JSON
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(traced_contacts, f, pretty_print=False)
