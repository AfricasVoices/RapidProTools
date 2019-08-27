import argparse

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)
log.set_project_name("SynchroniseContacts")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrates contacts from one Rapid Pro instance to another")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("instance_1_domain", metavar="instance-1-domain",
                        help="Domain that the first instance of Rapid Pro is running on")
    parser.add_argument("instance_1_credentials_url", metavar="instance-1-credentials-url",
                        help="GS URL to the organisation access token file for authenticating to the first instance")
    parser.add_argument("instance_2_domain", metavar="instance-2-domain",
                        help="Domain that the second instance of Rapid Pro is running on")
    parser.add_argument("instance_2_credentials_url", metavar="instance-2-credentials-url",
                        help="GS URL to the organisation access token file for authenticating to the second instance")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    instance_1_domain = args.instance_1_domain
    instance_1_credentials_url = args.instance_1_credentials_url
    instance_2_domain = args.instance_2_domain
    instance_2_credentials_url = args.instance_2_credentials_url

    # Initialise the two instances
    log.info("Downloading the access token for instance 1...")
    instance_1_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, instance_1_credentials_url).strip()
    instance_1 = RapidProClient(instance_1_domain, instance_1_token)

    log.info("Downloading the target instance access token...")
    instance_2_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, instance_2_credentials_url).strip()
    instance_2 = RapidProClient(instance_2_domain, instance_2_token)

    # Synchronise the contact fields
    log.info("Synchronising contact fields...")
    instance_1_fields = instance_1.get_fields()
    instance_2_fields = instance_2.get_fields()
    for field in instance_1_fields:
        if field.key not in {f.key for f in instance_2_fields}:
            instance_2.create_field(field.label)
    for field in instance_2_fields:
        if field.key not in {f.key for f in instance_1_fields}:
            instance_1.create_field(field.label)
    log.info("Contact fields synchronised")

    # Synchronise the contacts
    log.info("Synchronising contacts...")
    instance_1_contacts = instance_1.get_raw_contacts()
    instance_2_contacts = instance_2.get_raw_contacts()

    for contact in instance_1_contacts + instance_2_contacts:
        assert len(contact.urns) == 1

    instance_1_contacts_lut = {c.urns[0].split("#")[0]: c for c in instance_1_contacts}
    instance_2_contacts_lut = {c.urns[0].split("#")[0]: c for c in instance_2_contacts}

    # Update contacts in instance 1 but not in instance 2
    urns_unique_to_instance_1 = instance_1_contacts_lut.keys() - instance_2_contacts_lut.keys()
    for i, urn in enumerate(urns_unique_to_instance_1):
        log.info(f"Adding new contacts to instance 2: {i + 1}/{len(urns_unique_to_instance_1)}")
        contact = instance_1_contacts_lut[urn]
        instance_2.update_contact(contact.urns[0], contact.name, contact.fields)

    # Update contacts in instance 2 but not in instance 1
    urns_unique_to_instance_2 = instance_2_contacts_lut.keys() - instance_1_contacts_lut.keys()
    for i, urn in enumerate(urns_unique_to_instance_2):
        log.info(f"Adding new contacts to instance 1: {i + 1}/{len(urns_unique_to_instance_2)}")
        contact = instance_2_contacts_lut[urn]
        instance_1.update_contact(contact.urns[0], contact.name, contact.fields)

    # Update contacts in both instances
    urns_in_both_instances = instance_1_contacts_lut.keys() & instance_2_contacts_lut.keys()
    for i, urn in enumerate(urns_in_both_instances):
        contact_v1 = instance_1_contacts_lut[urn]
        contact_v2 = instance_2_contacts_lut[urn]

        if contact_v1.name == contact_v2.name and contact_v1.fields == contact_v2.fields:
            log.info(f"Synchronising contacts in both instances: {i + 1}/{len(urns_in_both_instances)} (contacts identical)")
            continue

        # Contacts differ. Assume the most recent contact is correct.
        # IMPORTANT: If the same contact has been changed on both Rapid Pro instances since the last sync was performed,
        #            the older changes will be overwritten.
        #            If contacts differ in each instance, interim updates on Rapid Pro may be overwritten.
        name = None
        fields = None
        if contact_v1.modified_on > contact_v2.modified_on:
            name = contact_v1.name
            fields = contact_v2.fields
            fields.update(contact_v1.fields)
        else:
            name = contact_v2.name
            fields = contact_v1.fields
            fields.update(contact_v2.fields)

        # Drop unnecessary field updates.
        # This reduces traffic, and the risk of overwriting fields which have been updated in Rapid Pro since the
        # contacts were fetched (large syncs can take hours to days)
        for f in list(fields):
            if contact_v1.fields[f] == contact_v2.fields[f]:
                del fields[f]
            if fields[f] is None:
                del fields[f]

        log.info(f"Synchronising contacts in both instances: {i + 1}/{len(urns_in_both_instances)} (contacts differ)")
        instance_1.update_contact(urn, name, fields)
        instance_2.update_contact(urn, name, fields)
