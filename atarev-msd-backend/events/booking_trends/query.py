from dataclasses import dataclass

from base.helpers.fields import Field
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from events.booking_trends.form import AggType, BookingTrendsForm


@dataclass
class BookingTrendsQuery:

    form: BookingTrendsForm
    host_carrier: str

    @property
    def query(self):
        dt_range = self.form.get_date_range()
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", self.form.get_origin()),
                convert_list_param_to_criteria("dest_code", self.form.get_destination()),
                convert_list_param_to_criteria("country_of_sale", self.form.get_pos()),
                convert_list_param_to_criteria("distribution_channel", self.form.get_sales_channel()),
                convert_list_param_to_criteria("dom_op_al_code", [self.host_carrier, *self.form.get_graph_carriers()]),
                # self.convert_list_param_to_criteria("seg_class", self.form.get_cabin()),
                {
                    "$and": [
                        {"trave_date": {"$gte": dt_range[0]}},
                        {"trave_date": {"$lte": dt_range[1]}},
                    ]
                },
            ]
        )

        if self.form.get_agg_type() == AggType.MONTHLY.value:
            groupby = {"travel_year": "$travel_year", "travel_month": "$travel_month", "dom_op_al_code": "$dom_op_al_code"}
            project = {
                "carrier_code": "$_id.dom_op_al_code",
                "travel_month": "$_id.travel_month",
                "travel_year": "$_id.travel_year",
                "pax": 1,
                "_id": 0,
            }
        else:
            groupby = {"travel_date": "$travel_date", "dom_op_al_code": "$dom_op_al_code"}
            project = {
                "carrier_code": "$_id.dom_op_al_code",
                "travel_date": "$_id.travel_date",
                "_id": 0,
                "pax": 1,
                **Field.date_as_string("str_travel_date", "_id.travel_date"),
            }

        pipelines = [
            {"$match": match},
            {"$group": {"_id": groupby, "pax": {"$sum": "$pax"}}},
            {"$project": project},
        ]

        return pipelines


@dataclass
class EventQuery:

    form: BookingTrendsForm
    host_carrier: str

    @property
    def query(self):

        dt_range = self.form.get_date_range()

        return merge_criterions(
            [
                convert_list_param_to_criteria("country_code", self.form.get_holiday_countries()),
                convert_list_param_to_criteria("airline_code", [self.host_carrier, *self.form.get_graph_carriers()]),
                {
                    "$and": [
                        {"start_date": {"$gte": dt_range[0]}},
                        {"start_date": {"$lte": dt_range[1]}},
                    ]
                },
            ]
        )
