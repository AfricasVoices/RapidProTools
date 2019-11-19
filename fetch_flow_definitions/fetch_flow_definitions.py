import argparse
import json

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from src import FirestoreWrapper
from storage.google_cloud import google_cloud_utils

from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads the definitions for all the flows being used by this "
                                                 "project, and uploads them to a bucket.")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firestore_credentials_url", metavar="firestore-credentials-url",
                        help="GS URL to the credentials file to use to access the Firestore instance containing "
                             "the operations statistics")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firestore_credentials_url = args.firestore_credentials_url

    log.info("Initialising the Firestore client...")
    firestore_credentials = json.loads(google_cloud_utils.download_blob_to_string(
                google_cloud_credentials_file_path, firestore_credentials_url))
    firestore_wrapper = FirestoreWrapper(firestore_credentials)

    log.info("Loading the active project details...")
    active_projects = firestore_wrapper.get_active_projects()
    log.info(f"Loaded the details for {len(active_projects)} active projects")

    for project in active_projects:
        log.info(f"Archiving the latest flow definitions for project {project.project_name}...")

        log.info("Downloading the Rapid Pro token file and initialising the Rapid Pro client...")
        rapid_pro_token = google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, project.rapid_pro_token_url).strip()
        rapid_pro = RapidProClient(project.rapid_pro_domain, rapid_pro_token)

        log.info("Downloading all the flow definitions for this instance...")
        flow_ids = rapid_pro.get_all_flow_ids()
        flow_definitions_request_timestamp = TimeUtils.utc_now_as_iso_string()
        flow_definitions = rapid_pro.get_flow_definitions_for_flow_ids(flow_ids)

        log.info("Uploading the flow definitions to a cloud bucket...")
        upload_url = f"{project.flow_definitions_upload_url_prefix}{flow_definitions_request_timestamp}.json"
        flow_definitions_json = json.dumps(flow_definitions.serialize())
        google_cloud_utils.upload_string_to_blob(google_cloud_credentials_file_path, upload_url, flow_definitions_json)
