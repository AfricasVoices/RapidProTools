import datetime
import json

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from temba_client.v2 import TembaClient


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

    def get_raw_runs_for_flow_id(self, flow_id, range_start_inclusive=None, range_end_exclusive=None,
                                 raw_export_log=None):
        """
        Gets the raw runs for the given flow_id from RapidPro.
        
        :param flow_id: Id of the flow to download the runs of.
        :type flow_id: str
        :param range_start_inclusive: Start of the date-range to download runs from. If set, only downloads runs
                                      last modified since that date, otherwise downloads from the beginning of time.
        :type range_start_inclusive: datetime.pyi | None
        :param range_end_exclusive: End of the date-range to download runs from. If set, only downloads runs
                                    last modified before that date, otherwise downloads until the end of time.
        :param raw_export_log: File to write the raw data downloaded during this function call to.
        :type raw_export_log: file-like | None
        :return: Raw runs downloaded from Rapid Pro.
        :rtype: list of temba_client.v2.types.Run
        """
        range_end_inclusive = None
        if range_end_exclusive is not None:
            range_end_inclusive = range_end_exclusive - datetime.timedelta(microseconds=1)

        print(f"Fetching raw runs for flow with id '{flow_id}'...")
        raw_runs = self.rapid_pro.get_runs(
            flow=flow_id, after=range_start_inclusive, before=range_end_inclusive).all(retry_on_rate_exceed=True)
        print(f"Fetched {len(raw_runs)} runs")

        if raw_export_log is not None:
            print(f"Logging {len(raw_runs)} fetched runs...")
            json.dump([contact.serialize() for contact in raw_runs], raw_export_log)
            raw_export_log.write("\n")
            print(f"Logged fetched contacts")
        else:
            print("Not logging the raw export (argument 'raw_export_log' was None)")

        # Sort in ascending order of modification date
        raw_runs = list(raw_runs)
        raw_runs.reverse()

        return raw_runs

    def get_raw_contacts(self, range_start_inclusive=None, range_end_exclusive=None, raw_export_log=None):
        """
        Gets the raw contacts from RapidPro.
        
        :param range_start_inclusive: Start of the date-range to download contacts from. If set, only downloads contacts
                                      last modified since that date, otherwise downloads from the beginning of time.
        :type range_start_inclusive: datetime.pyi | None
        :param range_end_exclusive: End of the date-range to download contacts from. If set, only downloads contacts
                                    last modified before that date, otherwise downloads until the end of time.
        :type range_end_exclusive: datetime.pyi | None
        :param raw_export_log: File to write the raw data downloaded during this function call to.
        :type raw_export_log: file-like | None
        :return: Raw contacts downloaded from Rapid Pro.
        :rtype: list of temba_client.v2.types.Contact
        """
        range_end_inclusive = None
        if range_end_exclusive is not None:
            range_end_inclusive = range_end_exclusive - datetime.timedelta(microseconds=1)
            
        print("Fetching raw contacts...")
        raw_contacts = self.rapid_pro.get_contacts(
            after=range_start_inclusive, before=range_end_inclusive).all(retry_on_rate_exceed=True)
        assert len(set(c.uuid for c in raw_contacts)) == len(raw_contacts), "Non-unique contact UUID in RapidPro"
        print(f"Fetched {len(raw_contacts)} contacts")

        if raw_export_log is not None:
            print(f"Logging {len(raw_contacts)} fetched contacts...")
            json.dump([contact.serialize() for contact in raw_contacts], raw_export_log)
            raw_export_log.write("\n")
            print(f"Logged fetched contacts")
        else:
            print("Not logging the raw export (argument 'raw_export_log' was None)")

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
        print(f"Filtered raw data for the latest objects. {len(latest_data)}/{len(raw_data)} items remain.")
        return latest_data

    def update_raw_data_with_latest_modified(self, get_fn, id_key, prev_raw_data=None, raw_export_log=None):
        """
        Updates a list of raw objects downloaded from Rapid Pro, by only downloading objects which have been 
        updated since that previous export was performed.

        :param get_fn: Function to call to retrieve the newer objects.
        :type get_fn: function of (range_start_inclusive, raw_export_log) -> list of temba_client.serialization.TembaObject
        :param id_key: A function that returns an id for each object (needed to filter for only the most recently
                       modified version of duplicated objects).
        :type id_key: function of temba_client.serialization.TembaObject -> hashable
        :param prev_raw_data: List of Rapid Pro objects from a previous export, or None.
                              If None, all objects will be downloaded.
        :type prev_raw_data: list of temba_client.serialization.TembaObject | None
        :param raw_export_log: File to write raw data fetched during the export to. 
                               Data is written in the format it came out of Rapid Pro.
        :type raw_export_log: file-like
        :return: Updated list of Rapid Pro objects.
        :rtype: list of temba_client.serialization.TembaObject
        """
        if prev_raw_data is not None:
            prev_raw_data = list(prev_raw_data)

        range_start_inclusive = None
        if prev_raw_data is not None and len(prev_raw_data) > 0:
            prev_raw_data.sort(key=lambda contact: contact.modified_on)
            range_start_inclusive = prev_raw_data[-1].modified_on + datetime.timedelta(microseconds=1)

        new_data = get_fn(range_start_inclusive=range_start_inclusive, raw_export_log=raw_export_log)

        all_raw_data = prev_raw_data + new_data
        return self.filter_latest(all_raw_data, id_key)
    
    def update_raw_contacts_with_latest_modified(self, prev_raw_contacts=None, raw_export_log=None):
        """
        Updates a list of contacts previously downloaded from Rapid Pro, by only fetching contacts which have been 
        updated since that previous export was performed.
        
        :param prev_raw_contacts: A list of Rapid Pro contact objects from a previous export, or None.
                                  If None, all contacts will be downloaded.
        :type prev_raw_contacts: list of temba_client.v2.types.Contact | None
        :param raw_export_log: File to write the newly retrieved contacts to.
        :type raw_export_log: file-like | None
        :return: Updated list of Rapid Pro Contact objects.
        :rtype: list of temba_client.v2.types.Contact
        """
        return self.update_raw_data_with_latest_modified(
            self.get_raw_contacts, lambda contact: contact.uuid,
            prev_raw_data=prev_raw_contacts, raw_export_log=raw_export_log
        )
    
    def update_raw_runs_with_latest_modified(self, flow_id, prev_raw_runs=None, raw_export_log=None):
        """
        Updates a list of runs previously downloaded from Rapid Pro, by only fetching runs which have been
        updated since that previous export was performed.

        :param flow_id: Id of flow to update.
        :type flow_id: str
        :param prev_raw_runs: A list of Rapid Pro run objects from a previous export, or None.
                              If None, all runs for the specified flow will be downloaded.
        :type prev_raw_runs: list of temba_client.v2.types.Run | None
        :param raw_export_log: File to write the newly retrieved runs to.
        :type raw_export_log: file-like | None
        :return: Updated list of Rapid Pro Run objects.
        :rtype: list of temba_client.v2.types.Run
        """
        return self.update_raw_data_with_latest_modified(
            lambda **kwargs: self.get_raw_runs_for_flow_id(flow_id, **kwargs), lambda run: run.id,
            prev_raw_data=prev_raw_runs, raw_export_log=raw_export_log
        )

    @staticmethod
    def convert_runs_to_traced_data(user, raw_runs, raw_contacts, phone_uuids, test_contacts=None):
        if test_contacts is None:
            test_contacts = []

        print(f"Converting {len(raw_runs)} raw runs to TracedData...")

        contacts_lut = {c.uuid: c for c in raw_contacts}

        traced_runs = []
        for run in raw_runs:
            if run.contact.uuid not in contacts_lut:
                # Sometimes contact uuids which appear in `runs` do not appear in `contact_runs`.
                # I have only observed this happen for contacts which were created very recently.
                # This test skips the run in this case; it should be included next time this script is executed.
                print(f"Warning: Run found with Rapid Pro Contact UUID '{run.contact.uuid}', "
                      f"but this id is not present in the downloaded contacts")
                continue

            contact_urns = contacts_lut[run.contact.uuid].urns
            if len(contact_urns) == 0:
                print(f"Warning: Ignoring contact with no urn. URNs: {contact_urns} "
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

            run_dict[f"created_on - {run.flow.name}"] = run.created_on.isoformat()
            run_dict[f"modified_on - {run.flow.name}"] = run.modified_on.isoformat()
            run_dict[f"exited_on - {run.flow.name}"] = None if run.exited_on is None else run.exited_on.isoformat()
            run_dict[f"exit_type - {run.flow.name}"] = run.exit_type

            traced_runs.append(
                TracedData(run_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

        print(f"Converted {len(traced_runs)} raw runs to TracedData")

        return traced_runs
