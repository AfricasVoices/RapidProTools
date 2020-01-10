import datetime
import gzip
import json
import random
import time
import urllib
from io import BytesIO

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from dateutil.relativedelta import relativedelta
from temba_client.exceptions import TembaRateExceededError
from temba_client.v2 import TembaClient, Broadcast, Run

log = Logger(__name__)


class RapidProClient(object):
    MAX_RETRIES = 5
    MAX_BACKOFF_POWER = 6
    
    def __init__(self, server, token):
        """
        :param server: Server hostname, e.g. 'rapidpro.io'
        :type server: str
        :param token: Organization API token
        :type token: str
        """
        self.rapid_pro = TembaClient(server, token)
        
    def list_archives(self, archive_type=None):
        """
        Lists all of the available archives on this instance.

        Returns a list of objects with archive metadata. Pass one of these metadata objects into
        `RapidProClient.get_archive` to download the archive itself. Note that the download links in the returned
        metadata are only valid for a short period of time. From experimentation as at January 2020,
        links are valid for 1 day after this call is made.
        
        :param archive_type: The type of archives to list (either 'message' or 'run') or None.
                             If None, lists both types of archive.
        :type archive_type: str | None
        :return: List of available archives on this instance.
        :rtype: list of temba_client.v2.types.Archive
        """
        assert archive_type in {"message", "run"}

        return self.rapid_pro.get_archives(archive_type=archive_type).all(retry_on_rate_exceed=True)

    def get_archive(self, archive_metadata):
        """
        Downloads the archive specified by an archive metadata object, and converts it into a valid list of Message
        or Run objects.
        
        Note: Deserializing to message objects is not yet implemented, so requests for message archives will fail
        with an assertion error.
        TODO: Support deserializing messages

        :param archive_metadata: Metadata for the archive. To obtain these, see `RapidProClient.list_archives`.
        :type archive_metadata: temba_client.v2.types.Archive
        :return: Data downloaded from the archive.
        :rtype: list of temba_client.v2.Message | list of temba_client.v2.Run
        """
        # Download the archive, which is in a gzipped JSONL format, and decompress.
        log.info(f"Downloading {archive_metadata.record_count} records from archive {archive_metadata.download_url}...")
        archive_response = urllib.request.urlopen(archive_metadata.download_url)
        raw_file = BytesIO(archive_response.read())
        decompressed_file = gzip.GzipFile(fileobj=raw_file)

        # Convert each of the decompressed results to a Run or Message object, depending on what the archive contains.
        results = []
        if archive_metadata.archive_type == "run":
            for line in decompressed_file.readlines():
                serialized_run = json.loads(line)

                # Set the 'start' field to null if it doesn't exist.
                # This field is required to be present in order to be able to deserialize runs, but is often not
                # present in the downloaded data (possibly because the archiving process removes null fields, but
                # I haven't verified this). Since this field is often null in the data that comes out of the runs
                # API directly, and we don't use this in the pipelines, just set missing entries to None.
                if "start" not in serialized_run:
                    serialized_run["start"] = None
                    
                results.append(Run.deserialize(serialized_run))
        else:
            # TODO: Support deserializing messages
            assert False, "Deserializing messages archives is not implemented yet"

        assert len(results) == archive_metadata.record_count

        return results

    def get_flow_id(self, flow_name):
        """
        Gets the id for the flow with the requested name.

        :param flow_name: Name of flow to retrieve the id of.
        :type flow_name: str
        :return: The Rapid Pro id for the given flow name.
        :rtype: str
        """
        flows = self.rapid_pro.get_flows().all(retry_on_rate_exceed=True)
        matching_flows = [f for f in flows if f.name == flow_name]

        if len(matching_flows) == 0:
            available_flow_names = [f.name for f in flows]
            raise KeyError(f"Requested flow not found on RapidPro (Available flows: {', '.join(available_flow_names)})")
        if len(matching_flows) > 1:
            raise KeyError("Non-unique flow name")

        return matching_flows[0].uuid

    def get_flow_ids(self, flow_names):
        """
        Gets the ids for a list of flow names.

        :param flow_names: Names of the flows to retrieve the ids of.
        :type flow_names: list of str
        :return: The Rapid Pro ids for the given flow names.
        :rtype: list of str
        """
        return [self.get_flow_id(name) for name in flow_names]

    def get_all_flow_ids(self):
        """
        Gets all the flow ids currently available on the Rapid Pro instance.
        
        :return: Ids of all flows on Rapid Pro instance.
        :rtype: list of str
        """
        return [f.uuid for f in self.rapid_pro.get_flows().all(retry_on_rate_exceed=True)]

    def get_flow_definitions_for_flow_ids(self, flow_ids):
        """
        Gets the definitions for the flows with the requested ids from Rapid Pro.

        :param flow_ids: Ids of the flows to export the definitions of.
        :type flow_ids: list of str
        :return: An export object containing all of the requested flows, their dependencies, and triggers.
        :rtype: temba_client.v2.types.Export
        """
        return self.rapid_pro.get_definitions(flows=flow_ids, dependencies="all")

    def get_raw_messages(self, created_after_inclusive=None, created_before_exclusive=None,
                         raw_export_log_file=None):
        """
        Gets the raw messages from RapidPro.

        :param created_after_inclusive: Start of the date-range to download contacts from.
                                        If set, only downloads messages created on Rapid Pro since that date,
                                        otherwise downloads from the beginning of time.
        :type created_after_inclusive: datetime.datetime | None
        :param created_before_exclusive: End of the date-range to download contacts from.
                                         If set, only downloads messages created on Rapid Pro before that date,
                                         otherwise downloads until the end of time.
        :type created_before_exclusive: datetime.datetime | None
        :param raw_export_log_file: File to write the raw data downloaded during this function call to as json.
        :type raw_export_log_file: file-like | None
        :return: Raw contacts downloaded from Rapid Pro.
        :rtype: list of temba_client.v2.types.Message
        """
        all_time_log = "" if created_after_inclusive is not None or created_before_exclusive is not None else " from all of time"
        after_log = "" if created_after_inclusive is None else f", modified after {created_after_inclusive.isoformat()} inclusive"
        before_log = "" if created_before_exclusive is None else f", modified before {created_before_exclusive.isoformat()} exclusive"
        log.info(f"Fetching raw messages{all_time_log}{after_log}{before_log}...")

        created_before_inclusive = None
        if created_before_exclusive is not None:
            created_before_inclusive = created_before_exclusive - datetime.timedelta(microseconds=1)

        raw_messages = self.rapid_pro.get_messages(after=created_after_inclusive, before=created_before_inclusive)\
            .all(retry_on_rate_exceed=True)

        log.info(f"Fetched {len(raw_messages)} messages")

        if raw_export_log_file is not None:
            log.info(f"Logging {len(raw_messages)} fetched messages...")
            json.dump([contact.serialize() for contact in raw_messages], raw_export_log_file)
            raw_export_log_file.write("\n")
            log.info(f"Logged fetched messages")
        else:
            log.debug("Not logging the raw export (argument 'raw_export_log_file' was None)")

        # Sort in ascending order of creation date
        raw_messages = list(raw_messages)
        raw_messages.reverse()

        return raw_messages

    def send_message_to_urn(self, message, target_urn):
        """
        Sends a message to the given URN.

        :param message: Text of the message to send.
        :type message: str
        :param target_urn: URN to send the message to.
        :type target_urn: str
        :return: Id of the Rapid Pro broadcast created for this send request.
                 This id may be used to check on the status of the broadcast by making further requests to Rapid Pro.
                 Note that this is a broadcast (to one person) because Rapid Pro does not support unicasting.
        :rtype: int
        """
        log.info("Sending a message to an individual...")
        log.debug(f"Sending to '{target_urn}' the message '{message}'...")
        response: Broadcast = self.rapid_pro.create_broadcast(message, urns=[target_urn])
        log.info(f"Message send request created with broadcast id {response.id}")
        return response.id

    def get_broadcast_for_broadcast_id(self, broadcast_id):
        """
        Gets the broadcast with the requested id from Rapid Pro.

        :param broadcast_id: Id of broadcast to download from Rapid Pro
        :type broadcast_id: int
        :return: Broadcast with id 'broadcast_id'
        :rtype: temba_client.v2.Broadcast
        """
        matching_broadcasts = self.rapid_pro.get_broadcasts(broadcast_id).all(retry_on_rate_exceed=True)
        assert len(matching_broadcasts) == 1, f"{len(matching_broadcasts)} broadcasts have id {broadcast_id} " \
            f"(expected exactly 1)"
        return matching_broadcasts[0]

    def _get_archived_runs_for_flow_id(self, flow_id, last_modified_after_inclusive=None,
                                       last_modified_before_exclusive=None):
        """
        Gets the raw runs for the given flow_id from Rapid Pro's archives.
        
        Uses the last_modified dates to determine which archives to download.

        :param flow_id: Id of the flow to download the runs of.
        :type flow_id: str
        :param last_modified_after_inclusive: Start of the date-range to download runs from.
                                              If set, only downloads runs last modified since that date,
                                              otherwise downloads from the beginning of time.
        :type last_modified_after_inclusive: datetime.datetime | None
        :param last_modified_before_exclusive: End of the date-range to download runs from.
                                               If set, only downloads runs last modified before that date,
                                               otherwise downloads until the end of time.
        :return: Raw runs downloaded from Rapid Pro's archives.
        :rtype: list of temba_client.v2.types.Run
        """
        runs = []
        for archive_metadata in self.list_archives("run"):
            # Determine the start and end dates for this archive
            archive_start_date = archive_metadata.start_date
            if archive_metadata.period == "daily":
                archive_end_date = archive_start_date + relativedelta(days=1, microseconds=-1)
            else:
                assert archive_metadata.period == "monthly"
                archive_end_date = archive_start_date + relativedelta(months=1, microseconds=-1)

            if (last_modified_after_inclusive is not None and archive_end_date < last_modified_after_inclusive) or \
                    (last_modified_before_exclusive is not None and archive_start_date >= last_modified_before_exclusive):
                log.debug(f"Skipping {archive_metadata.period} archive with date range {archive_start_date} - "
                          f"{archive_end_date}")
                continue

            for run in self.get_archive(archive_metadata):
                # Skip runs from flows other than the flow of interest
                if run.flow.uuid != flow_id:
                    continue

                # Skip runs from a datetime that is outside the date range of interest
                if (last_modified_after_inclusive is not None and run.modified_on < last_modified_after_inclusive) or \
                        (last_modified_before_exclusive is not None and run.modified_on >= last_modified_before_exclusive):
                    continue

                runs.append(run)

        return runs

    def get_raw_runs_for_flow_id(self, flow_id, last_modified_after_inclusive=None, last_modified_before_exclusive=None,
                                 raw_export_log_file=None):
        """
        Gets the raw runs for the given flow_id from Rapid Pro's live database and, if needed, from its archives.

        :param flow_id: Id of the flow to download the runs of.
        :type flow_id: str
        :param last_modified_after_inclusive: Start of the date-range to download runs from.
                                              If set, only downloads runs last modified since that date,
                                              otherwise downloads from the beginning of time.
        :type last_modified_after_inclusive: datetime.datetime | None
        :param last_modified_before_exclusive: End of the date-range to download runs from.
                                               If set, only downloads runs last modified before that date,
                                               otherwise downloads until the end of time.
        :param raw_export_log_file: File to write the raw data downloaded during this function call to,
                                    as serialised json.
        :type raw_export_log_file: file-like | None
        :return: Raw runs downloaded from Rapid Pro.
        :rtype: list of temba_client.v2.types.Run
        """
        all_time_log = "" if last_modified_after_inclusive is not None or last_modified_before_exclusive is not None else ", from all of time"
        after_log = "" if last_modified_after_inclusive is None else f", modified after {last_modified_after_inclusive.isoformat()} inclusive"
        before_log = "" if last_modified_before_exclusive is None else f", modified before {last_modified_before_exclusive.isoformat()} exclusive"
        log.info(f"Fetching raw runs for flow with id '{flow_id}'{all_time_log}{after_log}{before_log}...")

        last_modified_before_inclusive = None
        if last_modified_before_exclusive is not None:
            last_modified_before_inclusive = last_modified_before_exclusive - datetime.timedelta(microseconds=1)

        archived_runs = self._get_archived_runs_for_flow_id(
            flow_id, last_modified_after_inclusive=last_modified_after_inclusive,
            last_modified_before_exclusive=last_modified_before_exclusive
        )

        live_runs = self.rapid_pro.get_runs(
            flow=flow_id, after=last_modified_after_inclusive, before=last_modified_before_inclusive
        ).all(retry_on_rate_exceed=True)

        raw_runs = archived_runs + live_runs
        log.info(f"Fetched {len(raw_runs)} runs ({len(archived_runs)} from archives, {len(live_runs)} from production)")

        # Check that we only see each run once. This shouldn't be possible, due to
        # https://github.com/nyaruka/rp-archiver/blob/7d3430b5260fa92abb62d828fc526af8e9d9d50a/archiver.go#L624,
        # but this check exists to be safe.
        assert len(raw_runs) == len({run.id for run in raw_runs}), "Duplicate run found in the downloaded data. " \
                                                                   "This could be because a run with this id exists " \
                                                                   "in both the archives and live database."

        if raw_export_log_file is not None:
            log.info(f"Logging {len(raw_runs)} fetched runs...")
            json.dump([contact.serialize() for contact in raw_runs], raw_export_log_file)
            raw_export_log_file.write("\n")
            log.info(f"Logged fetched runs")
        else:
            log.debug("Not logging the raw export (argument 'raw_export_log_file' was None)")

        # Sort in ascending order of modification date
        raw_runs = list(raw_runs)
        raw_runs.sort(key=lambda run: run.modified_on)

        return raw_runs

    def get_raw_contacts(self, last_modified_after_inclusive=None, last_modified_before_exclusive=None,
                         raw_export_log_file=None):
        """
        Gets the raw contacts from RapidPro.

        :param last_modified_after_inclusive: Start of the date-range to download contacts from.
                                              If set, only downloads contacts last modified since that date,
                                              otherwise downloads from the beginning of time.
        :type last_modified_after_inclusive: datetime.datetime | None
        :param last_modified_before_exclusive: End of the date-range to download contacts from.
                                               If set, only downloads contacts last modified before that date,
                                               otherwise downloads until the end of time.
        :type last_modified_before_exclusive: datetime.datetime | None
        :param raw_export_log_file: File to write the raw data downloaded during this function call to as json.
        :type raw_export_log_file: file-like | None
        :return: Raw contacts downloaded from Rapid Pro.
        :rtype: list of temba_client.v2.types.Contact
        """
        all_time_log = "" if last_modified_after_inclusive is not None or last_modified_before_exclusive is not None else " from all of time"
        after_log = "" if last_modified_after_inclusive is None else f", modified after {last_modified_after_inclusive.isoformat()} inclusive"
        before_log = "" if last_modified_before_exclusive is None else f", modified before {last_modified_before_exclusive.isoformat()} exclusive"
        log.info(f"Fetching raw contacts{all_time_log}{after_log}{before_log}...")

        last_modified_before_inclusive = None
        if last_modified_before_exclusive is not None:
            last_modified_before_inclusive = last_modified_before_exclusive - datetime.timedelta(microseconds=1)

        raw_contacts = self.rapid_pro.get_contacts(
            after=last_modified_after_inclusive, before=last_modified_before_inclusive).all(retry_on_rate_exceed=True)
        assert len(set(c.uuid for c in raw_contacts)) == len(raw_contacts), "Non-unique contact UUID in RapidPro"

        log.info(f"Fetched {len(raw_contacts)} contacts")

        if raw_export_log_file is not None:
            log.info(f"Logging {len(raw_contacts)} fetched contacts...")
            json.dump([contact.serialize() for contact in raw_contacts], raw_export_log_file)
            raw_export_log_file.write("\n")
            log.info(f"Logged fetched contacts")
        else:
            log.debug("Not logging the raw export (argument 'raw_export_log_file' was None)")

        # Sort in ascending order of modification date
        raw_contacts = list(raw_contacts)
        raw_contacts.reverse()
        
        return raw_contacts

    @staticmethod
    def filter_latest(raw_data, id_key):
        """
        Filters raw data for the latest version of each object only.

        :param raw_data: Raw data to filter.
        :type raw_data: list of temba_client.serialization.TembaObject
        :param id_key: A function that returns an id for each object. Where multiple objects are found with the same id,
                       only the most recent is kept.
        :type id_key: function of temba_client.serialization.TembaObject -> hashable
        :return: Raw data, with only the latest version of each object.
        :rtype: list of temba_client.serialization.TembaObject
        """
        raw_data.sort(key=lambda obj: obj.modified_on)
        data_lut = dict()
        for x in raw_data:
            data_lut[id_key(x)] = x
        latest_data = list(data_lut.values())
        log.info(f"Filtered raw data for the latest objects. Returning {len(latest_data)}/{len(raw_data)} items.")
        return latest_data

    def update_raw_data_with_latest_modified(self, get_fn, id_key, prev_raw_data=None, raw_export_log_file=None):
        """
        Updates a list of raw objects downloaded from Rapid Pro, by only downloading objects which have been
        updated since that previous export was performed.

        :param get_fn: Function to call to retrieve the newer objects.
        :type get_fn: function of (range_start_inclusive, raw_export_log_file) ->
                          list of temba_client.serialization.TembaObject
        :param id_key: A function that returns an id for each object (needed to filter for only the most recently
                       modified version of duplicated objects).
        :type id_key: function of temba_client.serialization.TembaObject -> hashable
        :param prev_raw_data: List of Rapid Pro objects from a previous export, or None.
                              If None, all objects will be downloaded.
        :type prev_raw_data: list of temba_client.serialization.TembaObject | None
        :param raw_export_log_file: File to write raw data fetched during the export to as json.
        :type raw_export_log_file: file-like
        :return: Updated list of Rapid Pro objects.
        :rtype: list of temba_client.serialization.TembaObject
        """
        if prev_raw_data is not None:
            prev_raw_data = list(prev_raw_data)

        last_modified_after_inclusive = None
        if prev_raw_data is not None and len(prev_raw_data) > 0:
            prev_raw_data.sort(key=lambda contact: contact.modified_on)
            last_modified_after_inclusive = prev_raw_data[-1].modified_on + datetime.timedelta(microseconds=1)

        new_data = get_fn(last_modified_after_inclusive=last_modified_after_inclusive, raw_export_log_file=raw_export_log_file)

        all_raw_data = prev_raw_data + new_data
        return self.filter_latest(all_raw_data, id_key)
    
    def update_raw_contacts_with_latest_modified(self, prev_raw_contacts=None, raw_export_log_file=None):
        """
        Updates a list of contacts previously downloaded from Rapid Pro, by only fetching contacts which have been 
        updated since that previous export was performed.
        
        :param prev_raw_contacts: A list of Rapid Pro contact objects from a previous export, or None.
                                  If None, all contacts will be downloaded.
        :type prev_raw_contacts: list of temba_client.v2.types.Contact | None
        :param raw_export_log_file: File to write the newly retrieved contacts to as json.
        :type raw_export_log_file: file-like | None
        :return: Updated list of Rapid Pro Contact objects.
        :rtype: list of temba_client.v2.types.Contact
        """
        return self.update_raw_data_with_latest_modified(
            self.get_raw_contacts, lambda contact: contact.uuid,
            prev_raw_data=prev_raw_contacts, raw_export_log_file=raw_export_log_file
        )

    def update_raw_runs_with_latest_modified(self, flow_id, prev_raw_runs=None, raw_export_log_file=None):
        """
        Updates a list of runs previously downloaded from Rapid Pro, by only fetching runs which have been
        updated since that previous export was performed.

        :param flow_id: Id of flow to update.
        :type flow_id: str
        :param prev_raw_runs: A list of Rapid Pro run objects from a previous export, or None.
                              If None, all runs for the specified flow will be downloaded.
        :type prev_raw_runs: list of temba_client.v2.types.Run | None
        :param raw_export_log_file: File to write the newly retrieved runs to as json.
        :type raw_export_log_file: file-like | None
        :return: Updated list of Rapid Pro Run objects.
        :rtype: list of temba_client.v2.types.Run
        """
        return self.update_raw_data_with_latest_modified(
            lambda **kwargs: self.get_raw_runs_for_flow_id(flow_id, **kwargs), lambda run: run.id,
            prev_raw_data=prev_raw_runs, raw_export_log_file=raw_export_log_file
        )

    def update_contact(self, urn, name=None, contact_fields=None):
        """
        Updates a contact on the server.

        :param urn: URN of the contact to update.
        :type urn: str
        :param name: Name to update to or None. If None, the contact's name is not updated.
        :type name: str | None
        :param contact_fields: Dictionary of field key to new field value | None. If None, no keys are updated.
                               Keys present on the server contact but not in this dictionary are left unchanged.
        :type contact_fields: (dict of str -> str) | None
        """
        return self._retry_on_rate_exceed(lambda: self.rapid_pro.update_contact(urn, name=name, fields=contact_fields))

    def get_fields(self):
        """
        Fetches all the contact fields.

        :return: All contact fields.
        :rtype: list of temba_client.v2.types.Field
        """
        log.info("Fetching all fields...")
        fields = self.rapid_pro.get_fields().all(retry_on_rate_exceed=True)
        log.info(f"Downloaded {len(fields)} fields")
        return fields

    def create_field(self, label):
        """
        Creates a contact field with the given label.

        :param label: The name of the contact field to create.
        :type label: str
        :return: The contact field that was just created.
        :rtype: temba_client.v2.types.Field
        """
        log.info(f"Creating field '{label}'...")
        return self._retry_on_rate_exceed(lambda: self.rapid_pro.create_field(label, "text"))

    @classmethod
    def _retry_on_rate_exceed(cls, request):
        """
        Calls the given request function. If the Rapid Pro server fails with a rate exceeded error, retries up to 
        cls.MAX_RETRIES times using binary exponential backoff.
        
        This function is needed because while the Rapid Pro API supports auto-retrying on get requests,
        it does not for create/update/delete requests.
        
        :param request: Function which runs the request when called.
        :type request: function
        :return: Result of the request.
        :rtype: any
        """
        retries = 0
        while True:
            try:
                return request()
            except TembaRateExceededError as ex:
                retries += 1

                if retries < cls.MAX_RETRIES and ex.retry_after:
                    server_wait_time = ex.retry_after
                    backoff_wait_time = random.uniform(0, 2 ** (min(retries, cls.MAX_BACKOFF_POWER)))
                    
                    log.debug(f"Rate exceeded. Sleeping for {server_wait_time + backoff_wait_time} seconds")

                    time.sleep(server_wait_time + backoff_wait_time)
                else:
                    raise ex

    @staticmethod
    def convert_runs_to_traced_data(user, raw_runs, raw_contacts, phone_uuids, test_contacts=None):
        """
        Converts raw data fetched from Rapid Pro to TracedData.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param raw_runs: Raw run objects to convert to TracedData.
        :type raw_runs: list of temba_client.v2.types.Run
        :param raw_contacts: Raw contact objects to use when converting to TracedData.
        :type raw_contacts: list of temba_client.v2.types.Contact
        :param phone_uuids: Phone number <-> UUID table.
        :type phone_uuids: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
        :param test_contacts: Rapid Pro contact UUIDs of test contacts.
                              Runs from any of those test contacts will be tagged with {'test_run': True}
        :type test_contacts: list of str | None
        :return: Raw data fetched from Rapid Pro converted to TracedData.
        :rtype: list of TracedData
        """
        if test_contacts is None:
            test_contacts = []

        log.info(f"Converting {len(raw_runs)} raw runs to TracedData...")

        contacts_lut = {c.uuid: c for c in raw_contacts}

        runs_with_uuids = []
        phone_numbers = []
        for run in raw_runs:
            if run.contact.uuid not in contacts_lut:
                # Sometimes contact uuids which appear in `runs` do not appear in `contact_runs`.
                # I have only observed this happen for contacts which were created very recently.
                # This test skips the run in this case; it should be included next time this script is executed.
                log.warning(f"Run found with Rapid Pro Contact UUID '{run.contact.uuid}', "
                            f"but this id is not present in the downloaded contacts")
                continue

            contact_urns = contacts_lut[run.contact.uuid].urns
            if len(contact_urns) == 0:
                log.warning(f"Ignoring contact with no urn. URNs: {contact_urns} "
                            f"(Rapid Pro Contact UUID: {run.contact.uuid})")
                continue

            phone_numbers.append(PhoneCleaner.normalise_phone(contact_urns[0]))
            runs_with_uuids.append(run)

        phone_to_uuid_lut = phone_uuids.data_to_uuid_batch(phone_numbers)

        traced_runs = []
        for run in runs_with_uuids:
            contact_urns = contacts_lut[run.contact.uuid].urns
            run_dict = {
                "avf_phone_id": phone_to_uuid_lut[PhoneCleaner.normalise_phone(contact_urns[0])],
                f"run_id - {run.flow.name}": run.id
            }

            for category, response in run.values.items():
                run_dict[category.title() + " (Category) - " + run.flow.name] = response.category
                run_dict[category.title() + " (Value) - " + run.flow.name] = response.value
                # Convert from "input" to "text" here to match terminology in Rapid Pro's Excel exports.
                run_dict[category.title() + " (Text) - " + run.flow.name] = response.input
                run_dict[category.title() + " (Name) - " + run.flow.name] = response.name
                run_dict[category.title() + " (Time) - " + run.flow.name] = response.time.isoformat()
                run_dict[category.title() + " (Run ID) - " + run.flow.name] = run.id

            if run.contact.uuid in test_contacts:
                run_dict["test_run"] = True
            else:
                assert len(contact_urns) == 1, \
                    f"A non-test contact has multiple URNs (Rapid Pro Contact UUID: {run.contact.uuid})"

            run_dict[f"run_created_on - {run.flow.name}"] = run.created_on.isoformat()
            run_dict[f"run_modified_on - {run.flow.name}"] = run.modified_on.isoformat()
            run_dict[f"run_exited_on - {run.flow.name}"] = None if run.exited_on is None else run.exited_on.isoformat()
            run_dict[f"run_exit_type - {run.flow.name}"] = run.exit_type

            traced_runs.append(
                TracedData(run_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

        log.info(f"Converted {len(traced_runs)} raw runs to TracedData")

        return traced_runs
