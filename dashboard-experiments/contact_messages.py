import argparse
import time

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO
from temba_client.v2 import TembaClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download all messages exchanged with a contact")
    parser.add_argument("server", help="Base URL of server channel to connect to, including c/ex/<uuid>/", nargs=1)
    parser.add_argument("api_token", metavar="api-token", help="Rapid Pro API Token", nargs=1)
    parser.add_argument("phone", help="Phone number to download contacts of", nargs=1)

    args = parser.parse_args()
    server = args.server[0]
    api_token = args.api_token[0]
    contact_phone = args.phone[0]

    rapid_pro = TembaClient(server, api_token)

    # Get the uuid for a contact
    contacts = rapid_pro.get_contacts().all()
    matching_contacts = [c for c in contacts if "tel:{}".format(contact_phone) in c.urns]

    if len(matching_contacts) == 0:
        exit("No matching contacts")
    if len(matching_contacts) > 1:
        exit("Too many matching contacts")
    contact_uuid = matching_contacts[0].uuid

    # Download all messages to/from this user
    messages = rapid_pro.get_messages(contact=contact_uuid).all()

    # Sort the messages in ascending order of created date.
    messages.sort(key=lambda m: m.created_on)

    # Convert RapidPro Messages to TracedData
    data = []
    for message in messages:
        data.append(TracedData(
            {
                "contact_uuid": message.contact.uuid,
                "direction": message.direction,
                "created_on": message.created_on.isoformat(),
                "modified_on": message.modified_on.isoformat(),
                "sent_on": message.sent_on.isoformat(),
                "text": message.text,
                "type": message.type
            },
            Metadata("test_user", Metadata.get_call_location(), time.time())
        ))

    # Output for review, to CSV for now
    with open("{}.csv".format(contact_phone), "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(
            data, f, headers=["contact_uuid", "direction", "type", "created_on", "sent_on", "modified_on", "text"])
