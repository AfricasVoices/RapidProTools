import datetime

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from temba_client.v2 import TembaClient


class RapidProClient(object):
    def __init__(self, server, token):
        self.rapid_pro = TembaClient(server, token)
        
    def get_flow_id(self, flow_name):
        flows = self.rapid_pro.get_flows().all(retry_on_rate_exceed=True)
        matching_flows = [f for f in flows if f.name == flow_name]

        if len(matching_flows) == 0:
            available_flow_names = [f.name for f in flows]
            raise KeyError(f"Requested flow not found on RapidPro (Available flows: {', '.join(available_flow_names)})")
        if len(matching_flows) > 1:
            raise KeyError("Non-unique flow name")

        return matching_flows[0].uuid

    def get_flow_ids(self, flow_names):
        return [self.get_flow_id(name) for name in flow_names]

    def get_flow_definitions_for_flow_ids(self, flow_ids):
        return self.rapid_pro.get_definitions(flows=flow_ids, dependencies="all")

    def get_raw_runs_for_flow_id(self, flow_id, range_start_inclusive=None, range_end_exclusive=None):
        range_end_inclusive = None
        if range_end_exclusive is not None:
            range_end_inclusive = range_end_exclusive - datetime.timedelta(microseconds=1)

        print(f"Fetching raw runs for flow with id '{flow_id}'...")
        raw_runs = self.rapid_pro.get_runs(
            flow=flow_id, after=range_start_inclusive, before=range_end_inclusive).all(retry_on_rate_exceed=True)
        print(f"Fetched {len(raw_runs)} runs")

        # Sort in ascending order of modification date
        raw_runs = list(raw_runs)
        raw_runs.reverse()

        return raw_runs

    def get_raw_contacts(self, range_start_inclusive=None, range_end_exclusive=None):
        range_end_inclusive = None
        if range_end_exclusive is not None:
            range_end_inclusive = range_end_exclusive - datetime.timedelta(microseconds=1)
            
        print("Fetching raw contacts...")
        raw_contacts = self.rapid_pro.get_contacts(
            after=range_start_inclusive, before=range_end_inclusive).all(retry_on_rate_exceed=True)
        assert len(set(c.uuid for c in raw_contacts)) == len(raw_contacts), "Non-unique contact UUID in RapidPro"
        print(f"Fetched {len(raw_contacts)} contacts")
        
        # Sort in ascending order of modification date
        raw_contacts = list(raw_contacts)
        raw_contacts.reverse()
        
        return raw_contacts

    @staticmethod
    def filter_latest_raw_contacts(raw_contacts):
        raw_contacts.sort(key=lambda contact: contact.modified_on)
        contacts_lut = dict()
        for contact in raw_contacts:
            contacts_lut[contact.uuid] = contact
        latest_contacts = list(contacts_lut.values())
        print(f"Filtered contacts for the latest objects. {len(latest_contacts)}/{len(raw_contacts)} contacts remain.")
        return latest_contacts
    
    def update_raw_contacts_with_latest_modified(self, prev_raw_contacts=None):
        """
        Updates a list of contacts previously downloaded from Rapid Pro, by only fetching contacts which have been 
        updated since that previous export was performed.
        
        :param prev_raw_contacts: A list of Rapid Pro contact objects from a previous export, or None.
                                  If None, all contacts will be downloaded.
        :type prev_raw_contacts: list of temba_client.v2.types.Contact | None
        :return: Updated list of Rapid Pro Contact objects.
        :rtype: list of temba_client.v2.types.Contact
        """
        range_start_inclusive = None
        if prev_raw_contacts is not None and len(prev_raw_contacts) > 0:
            prev_raw_contacts.sort(key=lambda contact: contact.modified_on)
            range_start_inclusive = prev_raw_contacts[-1].modified_on + datetime.timedelta(microseconds=1)

        all_raw_contacts = prev_raw_contacts
        all_raw_contacts.extend(self.get_raw_contacts(range_start_inclusive=range_start_inclusive))
        
        return self.filter_latest_raw_contacts(all_raw_contacts)

    @staticmethod
    def convert_runs_to_traced_data(user, raw_runs, raw_contacts, phone_uuids, test_contacts=None):
        if test_contacts is None:
            test_contacts = []

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

        return traced_runs

    @staticmethod
    def coalesce_traced_runs_by_key(user, traced_runs, coalesce_key):
        coalesced_runs = dict()

        for run in traced_runs:
            if run[coalesce_key] not in coalesced_runs:
                coalesced_runs[run[coalesce_key]] = run
            else:
                coalesced_runs[run[coalesce_key]].append_data(
                    dict(run.items()), Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        return list(coalesced_runs.values())
