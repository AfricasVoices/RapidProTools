import datetime
import gzip
import json
import random
import time
import urllib
import warnings
from io import BytesIO

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils, IOUtils
from dateutil.relativedelta import relativedelta
from temba_client.exceptions import TembaRateExceededError, TembaHttpError
from temba_client.v2 import TembaClient, Broadcast, Run, Message

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

    def get_workspace_name(self):
        """
        :return: The name of this workspace.
        :rtype: str
        """
        return self.rapid_pro.get_org(retry_on_rate_exceed=True).name

    def get_workspace_uuid(self):
        """
        :return: The uuid of this workspace.
        :rtype: str
        """
        return self.rapid_pro.get_org(retry_on_rate_exceed=True).uuid
        
    def list_archives(self, archive_type=None):
        """
        Lists all of the available archives on this workspace.

        Returns a list of objects with archive metadata. Pass one of these metadata objects into
        `RapidProClient.get_archive` to download the archive itself. Note that the download links in the returned
        metadata are only valid for a short period of time. From experimentation as at January 2020,
        links are valid for 1 day after this call is made.
        
        :param archive_type: The type of archives to list (either 'message' or 'run') or None.
                             If None, lists both types of archive.
        :type archive_type: str | None
        :return: List of available archives on this workspace.
        :rtype: list of temba_client.v2.types.Archive
        """
        assert archive_type in {"message", "run"}

        return self.rapid_pro.get_archives(archive_type=archive_type).all(retry_on_rate_exceed=True)

    def get_archive(self, archive_metadata):
        """
        Downloads the archive specified by an archive metadata object, and converts it into a valid list of Message
        or Run objects.
        
        :param archive_metadata: Metadata for the archive. To obtain these, see `RapidProClient.list_archives`.
        :type archive_metadata: temba_client.v2.types.Archive
        :return: Data downloaded from the archive.
        :rtype: list of temba_client.v2.Message | list of temba_client.v2.Run
        """
        if archive_metadata.record_count == 0:
            log.info(f"Skipping empty archive {archive_metadata.start_date} ({archive_metadata.download_url})...")
            return []

        # Download the archive, which is in a gzipped JSONL format, and decompress.
        log.info(f"Downloading {archive_metadata.record_count} records from {archive_metadata.period} archive "
                 f"{archive_metadata.start_date} ({archive_metadata.download_url})...")
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
            assert archive_metadata.archive_type == "message", "Unsupported archive type, must be either 'run' or 'message'"
            for line in decompressed_file.readlines():
                serialized_msg = json.loads(line)

                results.append(Message.deserialize(serialized_msg))

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
        Gets all the flow ids currently available on this Rapid Pro workspace.
        
        :return: Ids of all flows on this Rapid Pro workspace.
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
    
    def _get_archived_messages(self, created_after_inclusive=None, created_before_exclusive=None):
        """
        Gets the raw messages from Rapid Pro's archives.
        
        Uses the created dates to determine which archives to download.
        Filtering is done on creation date because this is the only timestamp metadata field Rapid Pro supports filtering

        :param created_after_inclusive: Start of the date-range to download messages from.
                                        If set, only downloads messages created since that date,
                                        otherwise downloads from the beginning of time.
        :type created_after_inclusive: datetime.datetime | None
        :param created_before_exclusive: End of the date-range to download messages from.
                                        If set, only downloads messages created before that date,
                                        otherwise downloads until the end of time.
        :return: Raw messages downloaded from Rapid Pro's archives.
        :rtype: list of temba_client.v2.types.Message
        """
        messages = []
        for archive_metadata in self.list_archives("message"):
            # Determine the start and end dates for this archive
            archive_start_date = archive_metadata.start_date
            if archive_metadata.period == "daily":
                archive_end_date = archive_start_date + relativedelta(days=1, microseconds=-1)
            else:
                assert archive_metadata.period == "monthly"
                archive_end_date = archive_start_date + relativedelta(months=1, microseconds=-1)

            if (created_after_inclusive is not None and archive_end_date < created_after_inclusive) or \
                    (created_before_exclusive is not None and archive_start_date >= created_before_exclusive):
                print(f"archive_end_date: {archive_end_date} < ")
                print(f"created_after_inclusive: {created_after_inclusive}")
                print(f"created_before_exclusive: {created_before_exclusive}>=")
                print(f"archive_start_date: {archive_start_date} < ")

            for message in self.get_archive(archive_metadata):
                # Skip messages from a datetime that is outside the date range of interest
                if (created_after_inclusive is not None and message.modified_on < created_after_inclusive) or \
                        (created_before_exclusive is not None and message.modified_on >= created_before_exclusive):
                    continue

                messages.append(message)

        return messages

    def get_raw_messages(self, created_after_inclusive=None, created_before_exclusive=None,
                         raw_export_log_file=None, ignore_archives=False):
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

        if ignore_archives:
            log.debug(f"Ignoring messages in archives (because `ignore_archives` argument was set to True)")
            archived_messages = []
        else:
            archived_messages = self._get_archived_messages(
                created_after_inclusive=created_after_inclusive,
                created_before_exclusive=created_before_exclusive
            )

        log.info(f"Fetching messages from production Rapid Pro workspace...")
        production_messages = self.rapid_pro.get_messages(after=created_after_inclusive, before=created_before_inclusive)\
            .all(retry_on_rate_exceed=True)

        raw_messages = archived_messages + production_messages
        log.info(f"Fetched {len(raw_messages)} messages ({len(archived_messages)} from archives, "
                 f"{len(production_messages)} from production)")

        # Check that we only see each message once. 
        seen_message_ids = set()
        for message in raw_messages:
            assert message.id not in seen_message_ids, f"Duplicate message {message.id} found in the downloaded data. " \
                                                       f"This could be because a message with this id exists in both " \
                                                       f"the archives and the production database."
            seen_message_ids.add(message.id)

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

    def get_groups(self, uuid=None, name=None):
        """
        Gets all matching contact groups from a rapid_pro workspace

        :param uuid: group UUID to filter on. If None, returns all groups in the workspace.
        :type uuid: str | None
        :param name: group name to filter on. If None, returns all groups in the workspace.
        :type group name: str | None
        :return: List of groups matching the group query
        :rtype: list of temba_client.v2.types.Group
        """
        return [group for group in self.rapid_pro.get_groups(uuid=uuid, name=name).all(retry_on_rate_exceed=True)]

    def get_contacts(self, uuid=None, urn=None, group=None, deleted=None, before=None, after=None, reverse=None):
        """
        Gets all matching contacts from a rapid_pro workspace

        :param uuid: contact UUID to filter on. If None, returns all contacts in the workspace.
        :type uuid: str | None
        :param urn: contact URN
        :type urn: str | None
        :param group: contact group name or UUID to filter on. If None, returns all groups in the workspace.
        :type group: str | None
        :param deleted: return deleted contact only
        :type deleted: bool | None
        :param reverse: whether to return contacts ordered in reverse (oldest first).
        :type reverse: bool | None
        :param last_modified_before_exclusive:  Start of the date-range to download contacts from.
                                                If set, only downloads messages modified on Rapid Pro before that date,
                                                otherwise downloads from the beginning of time.
        :type last_modified_before_exclusive: datetime
        :param  last_modified_after_inclusive:  Start of the date-range to download contacts from.
                                                If set, only downloads messages modified on Rapid Pro after that date,
                                                otherwise downloads from the beginning of time.
        :type last_modified_after_inclusive: datetime
        :return: List of contacts who match the query
        :rtype: list of temba_client.v2.types.Group
        """
        return [contact for contact in self.rapid_pro.get_contacts(uuid=uuid, urn=urn, group=group, deleted=deleted,
                                                               before=before, after=after, reverse=reverse).all(
                                                                                            retry_on_rate_exceed=True)]

    def send_message_to_urn(self, message, target_urn, interrupt=False):
        """
        Sends a message to the given URN.

        :param message: Text of the message to send.
        :type message: str
        :param target_urn: URN to send the message to.
        :type target_urn: str
        :param interrupt: Whether to interrupt the target_urn from flows before sending the message.
        :type interrupt: bool
        :return: Id of the Rapid Pro broadcast created for this send request.
                 This id may be used to check on the status of the broadcast by making further requests to Rapid Pro.
                 Note that this is a broadcast (to one person) because Rapid Pro does not support unicasting.
        :rtype: int
        """
        if interrupt:
            self.interrupt_urns([target_urn])
        
        log.info("Sending a message to an individual...")
        log.debug(f"Sending to '{target_urn}' the message '{message}'...")
        response = self.rapid_pro.create_broadcast(message, urns=[target_urn])
        log.info(f"Message send request created with broadcast id {response.id}")
        return response.id

    def send_message_to_urns(self, message, target_urns, interrupt=False):
        """
        Sends a message to URNs.

        :param message: Text of the message to send.
        :type message: str
        :param target_urns: URNs to send the message to.
        :type target_urns: str
        :param interrupt: Whether to interrupt the target_urns from flows before sending the message.
        :type interrupt: bool
        :return: Ids of the Rapid Pro broadcasts created for this send request.
                 These ids may be used to check on the status of the broadcast by making further requests to Rapid Pro.
                 e.g. using get_broadcast_for_broadcast_id.
        :rtype: list of int
        """
        urns = target_urns
        log.info(f"Sending a message to {len(urns)} URNs...")
        log.debug(f"Sending to {urns}...")
        batch = []
        broadcast_ids = []
        interrupted = 0
        sent = 0

        for urn in urns:
            batch.append(urn)
            if len(batch) >= 100:  # limit of 100 imposed by Rapid Pro's API
                if interrupt:
                    self.rapid_pro.bulk_interrupt_contacts(batch)
                    interrupted += len(batch)
                    log.info(f"Interrupted {interrupted} / {len(urns)} URNs")

                response = self.rapid_pro.create_broadcast(message, urns=batch)
                broadcast_ids.append(response.id)
                sent += len(batch)
                batch = []
                log.info(f"Sent {sent} / {len(urns)} URNs")
        if len(batch) > 0:
            if interrupt:
                self.rapid_pro.bulk_interrupt_contacts(batch)
                interrupted += len(batch)
            response: Broadcast = self.rapid_pro.create_broadcast(message, urns=batch)
            sent += len(batch)
            broadcast_ids.append(response.id)
            log.info(f"Interrupted {interrupted} / {len(urns)} URNs")
            log.info(f"Sent {sent} / {len(urns)} URNs")

        log.info(f"Message send request created with broadcast ids {broadcast_ids}")
        return broadcast_ids

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

    def interrupt_urns(self, urns):
        """
        Interrupts the given URNs from the flows they are currently in, if any.

        If the list of URNs contains more than 100 items, requests will be made in batches of 100 URNs at a time.

        :param urns: URNs to interrupt
        :type urns: list of str
        """
        log.info(f"Interrupting {len(urns)} URNs...")
        log.debug(f"Interrupting {urns}...")
        batch = []
        interrupted = 0
        for urn in urns:
            batch.append(urn)
            if len(batch) >= 100:  # limit of 100 imposed by Rapid Pro's API
                self.rapid_pro.bulk_interrupt_contacts(batch)
                interrupted += len(batch)
                log.info(f"Interrupted {interrupted} / {len(urns)} URNs")
                batch = []
        if len(batch) > 0:
            self.rapid_pro.bulk_interrupt_contacts(batch)
            interrupted += len(batch)
            log.info(f"Interrupted {interrupted} / {len(urns)} URNs")

    def _get_archived_runs(self, flow_id=None, last_modified_after_inclusive=None,
                           last_modified_before_exclusive=None):
        """
        Gets the raw runs for the given flow_id from Rapid Pro's archives.
        
        Uses the last_modified dates to determine which archives to download.

        :param flow_id: Id of the flow to download the runs of. If None, downloads runs from all flows.
        :type flow_id: str | None
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
                if flow_id is not None and run.flow.uuid != flow_id:
                    continue

                # Skip runs from a datetime that is outside the date range of interest
                if (last_modified_after_inclusive is not None and run.modified_on < last_modified_after_inclusive) or \
                        (last_modified_before_exclusive is not None and run.modified_on >= last_modified_before_exclusive):
                    continue

                runs.append(run)

        return runs

    def get_raw_runs(self, flow_id=None, last_modified_after_inclusive=None, last_modified_before_exclusive=None,
                     raw_export_log_file=None, ignore_archives=False):
        """
        Gets the raw runs for the given flow_id from Rapid Pro's production database and, if needed, from its archives.

        :param flow_id: Id of the flow to download the runs of. If None, returns runs from all flows.
        :type flow_id: str | None
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
        :param ignore_archives: If True, skips downloading runs from Rapid Pro's archives.
        :type ignore_archives: bool
        :return: Raw runs downloaded from Rapid Pro.
        :rtype: list of temba_client.v2.types.Run
        """
        flow_id_log = "from all flows" if flow_id is None else f"from flow with id {flow_id}"
        all_time_log = "" if last_modified_after_inclusive is not None or last_modified_before_exclusive is not None else ", from all of time"
        after_log = "" if last_modified_after_inclusive is None else f", modified after {last_modified_after_inclusive.isoformat()} inclusive"
        before_log = "" if last_modified_before_exclusive is None else f", modified before {last_modified_before_exclusive.isoformat()} exclusive"
        log.info(f"Fetching raw runs {flow_id_log}{all_time_log}{after_log}{before_log}...")

        last_modified_before_inclusive = None
        if last_modified_before_exclusive is not None:
            last_modified_before_inclusive = last_modified_before_exclusive - datetime.timedelta(microseconds=1)

        if ignore_archives:
            log.debug(f"Ignoring runs in archives (because `ignore_archives` argument was set to True)")
            archived_runs = []
        else:
            archived_runs = self._get_archived_runs(
                flow_id=flow_id, last_modified_after_inclusive=last_modified_after_inclusive,
                last_modified_before_exclusive=last_modified_before_exclusive
            )

        log.info(f"Fetching runs from production Rapid Pro workspace...")
        production_runs = self.rapid_pro.get_runs(
            flow=flow_id, after=last_modified_after_inclusive, before=last_modified_before_inclusive
        ).all(retry_on_rate_exceed=True)

        raw_runs = archived_runs + production_runs
        log.info(f"Fetched {len(raw_runs)} runs ({len(archived_runs)} from archives, "
                 f"{len(production_runs)} from production)")

        # Check that we only see each run once. This shouldn't be possible, due to
        # https://github.com/nyaruka/rp-archiver/blob/7d3430b5260fa92abb62d828fc526af8e9d9d50a/archiver.go#L624,
        # but this check exists to be safe.
        seen_run_ids = set()
        for run in raw_runs:
            assert run.id not in seen_run_ids, f"Duplicate run {run.id} found in the downloaded data. This could be " \
                                               f"because a run with this id exists in both the archives and the " \
                                               f"production database."
            seen_run_ids.add(run.id)

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

    def get_raw_runs_for_flow_id(self, flow_id, last_modified_after_inclusive=None, last_modified_before_exclusive=None,
                                 raw_export_log_file=None, ignore_archives=False):
        warnings.warn("RapidProClient.get_raw_runs_for_flow_id is deprecated; use get_raw_runs instead")
        return self.get_raw_runs(flow_id, last_modified_after_inclusive, last_modified_before_exclusive,
                                 raw_export_log_file, ignore_archives)

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
        if prev_raw_data is None:
            prev_raw_data = []
        else:
            prev_raw_data = list(prev_raw_data)

        last_modified_after_inclusive = None
        if len(prev_raw_data) > 0:
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

    def update_raw_runs_with_latest_modified(self, flow_id, prev_raw_runs=None, raw_export_log_file=None,
                                             ignore_archives=False):
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
        :param ignore_archives: If True, skips downloading runs from Rapid Pro's archives.
        :type ignore_archives: bool
        :return: Updated list of Rapid Pro Run objects.
        :rtype: list of temba_client.v2.types.Run
        """
        return self.update_raw_data_with_latest_modified(
            lambda **kwargs: self.get_raw_runs(flow_id, ignore_archives=ignore_archives, **kwargs),
            lambda run: run.id, prev_raw_data=prev_raw_runs, raw_export_log_file=raw_export_log_file
        )

    def update_contact(self, urn, name=None, contact_fields=None, groups=None):
        """
        Updates a contact on the server.

        :param urn: URN of the contact to update.
        :type urn: str
        :param name: Name to update to or None. If None, the contact's name is not updated.
        :type name: str | None
        :param contact_fields: Dictionary of field key to new field value | None. If None, no keys are updated.
                               Keys present on the server contact but not in this dictionary are left unchanged.
        :type contact_fields: (dict of str -> str) | None
        :param groups: list of group objects or UUIDs. This will overwrite the groups in rapid_pro. If you intend to add
                                                       fetch the contact and append the group to the existing groups list.
        :type groups: list | None
        """
        return self._retry_on_rate_exceed(lambda: self.rapid_pro.update_contact(urn, name=name,
                                                                                fields=contact_fields, groups=groups))

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

    def create_field(self, label, field_id=None):
        """
        Creates a contact field with the given label.

        :param label: The name of the contact field to create.
        :type label: str
        :param field_id: The id to request Rapid Pro to use for the new contact field. This must be in a format
                         which Rapid Pro will accept, otherwise the created id may differ and this function will
                         fail.
        :type field_id: str
        :return: The contact field that was just created.
        :rtype: temba_client.v2.types.Field
        """
        if field_id is None:
            log.info(f"Creating field with label '{label}'...")
            rapid_pro_field = self._retry_on_rate_exceed(lambda: self.rapid_pro.create_field(label, "text"))
            log.info(f"Created field with id '{rapid_pro_field.key}'")
            return rapid_pro_field
        else:
            # Rapid Pro allows fields to be overwritten if they already exist.
            # Check if the requested field id exists, and fail if it does.
            fields = self.rapid_pro.get_fields(key=field_id).all(retry_on_rate_exceed=True)
            assert len(fields) == 0, f"Field with id '{field_id}' already exists in workspace"

            # Create a field with the requested id. Rapid Pro doesn't allow us to specify the field id, but they're
            # predictably generated from the label, so create a new field by setting the label to the field id
            # we want. Rapid Pro uses underscores in its field ids but doesn't accept them in label names, so replace
            # underscores with spaces first.
            initial_label = field_id.replace("_", " ").lower()
            log.info(f"Creating field with label '{initial_label}', to ensure the field id is '{field_id}'...")
            rapid_pro_field = self._retry_on_rate_exceed(lambda: self.rapid_pro.create_field(initial_label, "text"))
            log.info(f"Created field with id '{rapid_pro_field.key}'")
            assert rapid_pro_field.key == field_id, \
                f"The field id created by Rapid Pro, '{rapid_pro_field.key}', differs from the requested id " \
                f"'{field_id}'. Please clean up the problematic field in Rapid Pro, and try again making sure " \
                f"you request a valid id."

            # Having created a field with the desired id, update its label to the one requested.
            log.info(f"Updating field with id '{rapid_pro_field.key}' to have label '{label}'...")
            rapid_pro_field = self._retry_on_rate_exceed(lambda: self.rapid_pro.update_field(rapid_pro_field, label, "text"))
            log.info(f"Done. Created field with label '{rapid_pro_field.label}' and id '{rapid_pro_field.key}'")

            return rapid_pro_field

    def create_group(self, name):
        """
        Creates a new contact group in a rapid_pro workspace. If the group exists, rapid pro will add a suffix 1,2.. and
        create a group with the modified name.

        :param name: group name.
        :type name: str.
        :return: the new group.
        :rtype: temba_client.v2.types.Group
        """
        return self._retry_on_rate_exceed(lambda: self.rapid_pro.create_group(name=name))


    def create_contact(self, name=None, language=None, urns=None, contact_fields=None, groups=None):
        """
        Creates a new contact in a rapid_pro workspace.

        :param name: the full name of the contact
        :type name: str | None
        :param language: the contact language iso code, e.g. "eng"
        :type language: str | None
        :param urns: list of URN strings, e.g ['telegram:123456']
        :type urns: list | None
        :param contact_fields: contact fields to set or update
        :type contact_fields: dict of contact_field key -> value | None
        :param groups: list of group objects, UUIDs or names
        :type groups: list | None
        :return: the new contact
        :rtype: temba_client.v2.types.Contact
        """
        return self._retry_on_rate_exceed(lambda: self.rapid_pro.create_contact(name=name, language=language, urns=urns,
                                                                                fields=contact_fields, groups=groups))

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

                if retries >= cls.MAX_RETRIES or not ex.retry_after:
                    raise ex

                server_wait_time = ex.retry_after
                backoff_wait_time = random.uniform(0, 2 ** (min(retries, cls.MAX_BACKOFF_POWER)))

                log.debug(f"Rate exceeded. Sleeping for {server_wait_time + backoff_wait_time} seconds")

                time.sleep(server_wait_time + backoff_wait_time)
            except TembaHttpError as ex:
                retries += 1

                if retries >= cls.MAX_RETRIES:
                    raise ex

                if ex.caused_by.response.status_code == 504:
                    # (Don't log the details in the error message, because the detail string contains a URL which may
                    # include a phone number)
                    log.debug(f"TembaHttpError 504, retrying...")

                if ex.caused_by.response.status_code == 500:
                    log.debug(f"TembaHttpError 500, retrying...")

                else:
                    raise ex

    def export_all_data(self, export_dir_path):
        """
        Exports all the data available from Rapid Pro's API, including archives, to the specified directory.

        Caveats:
         - This doesn't export data which is marked in Rapid Pro as archived, e.g. archived flows or triggers. There's
           no way of getting archived data out of Rapid Pro without first manually un-archiving it. Note that this
           applies only to data marked as archived in the UI, and is different to Rapid Pro's automated archiving
           of older runs and messages, which *are* included in the export.
         - This doesn't export data at the templates, ticketers, or workspace endpoints because these aren't supported
           by the Rapid Pro python client library. These features are new and unused by AVF.
         - There's no underlying 'export all data' API provided by Rapid Pro, so this only exports data from endpoints
           which were known about last time this function was updated.

        :param export_dir_path: Directory to export the data to.
        :type export_dir_path: str
        """
        IOUtils.ensure_dirs_exist(export_dir_path)

        # Export the straightforward cases
        endpoints = {
            "boundaries": self.rapid_pro.get_boundaries,
            "broadcasts": self.rapid_pro.get_broadcasts,
            "campaigns": self.rapid_pro.get_campaigns,
            "campaign_events": self.rapid_pro.get_campaign_events,
            "channels": self.rapid_pro.get_channels,
            "channel_events": self.rapid_pro.get_channel_events,
            "classifiers": self.rapid_pro.get_classifiers,
            "contacts": self.rapid_pro.get_contacts,
            "fields": self.rapid_pro.get_fields,
            "flows": self.rapid_pro.get_flows,
            "flow_starts": self.rapid_pro.get_flow_starts,
            "globals": self.rapid_pro.get_globals,
            "groups": self.rapid_pro.get_groups,
            "labels": self.rapid_pro.get_labels,
            "resthooks": self.rapid_pro.get_resthooks,
            "resthook_events": self.rapid_pro.get_resthook_events,
            "resthook_subscribers": self.rapid_pro.get_resthook_subscribers,
        }
        for endpoint, export_func in endpoints.items():
            export_file_path = f"{export_dir_path}/{endpoint}.jsonl"
            log.info(f"Exporting {endpoint} to '{export_file_path}'...")

            with open(export_file_path, "w") as f:
                items_exported = 0
                for batch in export_func().iterfetches(retry_on_rate_exceed=True):
                    for item in batch:
                        items_exported += 1
                        f.write(json.dumps(item.serialize()) + "\n")
                log.info(f"Done. Exported {items_exported} {endpoint}")

        # Now handle the special cases...

        # Export endpoints which have archives, using the relevant RapidProClient 'get' functions because these handle
        # fetching from archives transparently.
        endpoints_with_archives = {
            "messages": self.get_raw_messages,
            "runs": self.get_raw_runs
        }
        for endpoint, export_func in endpoints_with_archives.items():
            export_file_path = f"{export_dir_path}/{endpoint}.jsonl"
            with open(export_file_path, "w") as f:
                log.info(f"Exporting {endpoint}, including those in archives, to {export_file_path}...")
                items_exported = 0
                for item in export_func():
                    f.write(json.dumps(item.serialize()) + "\n")
                    items_exported += 1
                log.info(f"Done. Exported {items_exported} {endpoint}")

        # Export the org data, which needs special treatment because it's not a list.
        export_file_path = f"{export_dir_path}/org.json"
        log.info(f"Exporting org to '{export_file_path}'...")
        org = self.rapid_pro.get_org(retry_on_rate_exceed=True)
        with open(export_file_path, "w") as f:
            f.write(json.dumps(org.serialize()))
        log.info(f"Done. Exported org")

        # Export the definitions data, which needs special treatment because this endpoint returns no data by default
        # (unlike all the other endpoints which return everything by default).
        export_file_path = f"{export_dir_path}/definitions.json"
        log.info(f"Exporting definitions to '{export_file_path}'")
        all_flow_ids = self.get_all_flow_ids()
        definitions = self.get_flow_definitions_for_flow_ids(all_flow_ids)
        with open(export_file_path, "w") as f:
            f.write(json.dumps(definitions.serialize()))
        log.info(f"Done. Exported definitions for {len(definitions.flows)} flows, {len(definitions.campaigns)} "
                 f"campaigns, and {len(definitions.triggers)} triggers")

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
                "urn_type": contact_urns[0].split(":")[0],
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
