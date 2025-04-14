from dataclasses import dataclass

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions

from .form import ReportForm


@dataclass
class ReportQuery:
    form: ReportForm
    host_code: str

    @property
    def query(self):
        conditions = [
            convert_list_param_to_criteria("old_authorization_value", {"$exists": True}),
            convert_list_param_to_criteria("cabin_params.origin", self.form.get_origin()),
            convert_list_param_to_criteria("cabin_params.destination", self.form.get_destination()),
            convert_list_param_to_criteria("cabin_params.airline_code", self.host_code),
            convert_list_param_to_criteria("cabin_params.cabin_code", self.form.get_cabin()),
            convert_list_param_to_criteria("cabin_params.flight_number", self.form.get_flight_keys()),
            convert_list_param_to_criteria("cabin_params.departure_date", self.form.get_outbound_date()),
        ]

        return merge_criterions(conditions)
