import datetime
import json

from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from temba_client.v2 import TembaClient, Broadcast

log = Logger(__name__)


class RapidProClient(object):
    def __init__(self, server, token):
        """
        :param server: Server hostname, e.g. 'rapidpro.io'
        :type server: str
        :param token: Organization API token
        :type token: str
        """
        self.rapid_pro = TembaClient(server, token)
        
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

    def get_flow_definitions_for_flow_ids(self, flow_ids):
        """
        Gets the definitions for the flows with the requested ids from Rapid Pro.

        :param flow_ids: Ids of the flows to export the definitions of.
        :type flow_ids: list of str
        :return: An export object containing all of the requested flows, their dependencies, and triggers.
        :rtype: temba_client.v2.types.Export
        """
        return self.rapid_pro.get_definitions(flows=flow_ids, dependencies="all")

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

    def get_raw_runs_for_flow_id(self, flow_id, last_modified_after_inclusive=None, last_modified_before_exclusive=None,
                                 raw_export_log_file=None):
        """
        Gets the raw runs for the given flow_id from RapidPro.

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

        raw_runs = self.rapid_pro.get_runs(
            flow=flow_id, after=last_modified_after_inclusive, before=last_modified_before_inclusive).all(retry_on_rate_exceed=True)

        log.info(f"Fetched {len(raw_runs)} runs")

        if raw_export_log_file is not None:
            log.info(f"Logging {len(raw_runs)} fetched runs...")
            json.dump([contact.serialize() for contact in raw_runs], raw_export_log_file)
            raw_export_log_file.write("\n")
            log.info(f"Logged fetched contacts")
        else:
            log.debug("Not logging the raw export (argument 'raw_export_log_file' was None)")

        # Sort in ascending order of modification date
        raw_runs = list(raw_runs)
        raw_runs.reverse()

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
        :type phone_uuids: core_data_modules.util.PhoneNumberUuidTable
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

        traced_runs = []
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

            run_dict = {
                "avf_phone_id": phone_uuids.add_phone(contact_urns[0]),
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
