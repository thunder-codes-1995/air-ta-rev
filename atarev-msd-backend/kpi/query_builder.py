from base.mongo_utils import merge_criterions, convert_list_param_to_criteria
from dds.builder import DdsBuilder
from dds.forms import (
    BaseMSDFilterParams,
)


class KpiQueryBuilder(DdsBuilder):

    def get_base_match_pgsdemo(self, form: BaseMSDFilterParams, ignore_parameters=[]):
        origin_airports = form.get_orig_city_airports_list()
        destination_airports = form.get_dest_city_airports_list()
        match = merge_criterions(
            [convert_list_param_to_criteria("orig_code", origin_airports),
             convert_list_param_to_criteria("dest_code", destination_airports)]
        )
        for ignore_parameter in ignore_parameters:
            match.pop(ignore_parameter, None)

        return match
