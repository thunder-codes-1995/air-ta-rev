from datetime import date
from typing import List, Tuple

from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired, Optional, Regexp

from base.constants import Constants
from base.forms import BaseParams, Form
from utils.rules import comma_separated_characters_only, date_range_rule, one_of


class BaseMSDFilterParams(BaseParams):
    # Required fields
    orig_region = StringField("orig_region", [])
    dest_region = StringField("dest_region", [])
    orig_country = StringField("orig_country", [])
    dest_country = StringField("dest_country", [])
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])

    pax_type = StringField("pax_type")

    # Optional fields
    pos = StringField("pos", [Optional()])
    selected_competitors = StringField(
        "selected_competitors", [Optional(), *comma_separated_characters_only("selected_competitors")]
    )
    main_competitor = StringField("main_competitor", [Optional()])
    cabin = StringField("cabin", [*comma_separated_characters_only("cabin"), Optional()])
    sales_channel = StringField("sales_channel", [Optional()])
    selected_yearmonth = StringField("selected_yearmonth", [Optional()] + date_range_rule("selected_yearmonth", day=False))
    agg_view = StringField("agg_view", [Optional()])

    # Below are helper functions to get data from request but already in a format that can be easily used in mongo queries
    # For example:
    #   "orig_city_airport" parameter can be comma separated string with airport codes
    #           so instead of parsing it and converting to array each time in multiple places, helper does it for you
    #   "date_range_start" is a date parameter and could already be converted once

    def get_orig_city_airports_list(self):
        """Return list of airport codes sent with parameter: 'orig_city_airport'"""
        return [] if self.orig_city_airport.data == None else self.orig_city_airport.data.split(",")

    def get_orig_country_list(self):
        """Return list of country codes sent with parameter: 'orig_country'"""
        return [] if self.orig_country.data == None else self.orig_country.data.split(",")

    def get_orig_regions_list(self):
        """Return list of region codes sent with parameter: 'orig_region'"""
        return [] if self.orig_region.data == None else self.orig_region.data.split(",")

    def get_dest_city_airports_list(self):
        """Return list of airport codes sent with parameter: 'dest_city_airport'"""
        return [] if self.dest_city_airport.data == None else self.dest_city_airport.data.split(",")

    def get_graph_carriers(self):
        """Return list of airport codes sent with parameter: 'graph_carriers'"""
        return (
            [*self.graph_carriers.data.split(","), request.user.carrier] if self.graph_carriers.data else [request.user.carrier]
        )

    def get_dest_country_list(self):
        """Return list of country codes sent with parameter: 'dest_country'"""
        return [] if self.dest_country.data == None else self.dest_country.data.split(",")

    def get_dest_regions_list(self):
        """Return list of airport codes sent with parameter: 'dest_region'"""
        return [] if self.dest_region.data == None else self.dest_region.data.split(",")

    def get_cabins_list(self):
        """Return list of cabins codes sent with parameter: 'cabin'"""
        if not self.cabin.data:
            return []
        return self.cabin.data.split(",")

    def get_class_list(self):
        """Return list of classes sent with parameter: 'cabin' but mapped to class names
        Example: 'cabin' parameter was ECO, 'class' should be 'Discount Economy Class'"""
        CABIN_CONV = {
            "FIRST": "First Class",
            "BUS": "Business Class",
            "PRE": "Premium/Full Economy Class",
            "ECO": "Discount Economy Class",
            "OTH": "Other Classes",
        }
        if Constants.ALL in self.get_cabins_list():
            return []
        class_names = [CABIN_CONV[e] for e in self.get_cabins_list()]
        return class_names

    def get_sales_channels_list(self):
        """Return list of carrier codes sent with parameter: 'sales_channel'"""
        sales_channel_list = self.sales_channel.data.split(",") if self.sales_channel.data else [Constants.ALL]
        if Constants.ALL in sales_channel_list:
            return []
        else:
            return sales_channel_list

    def get_main_competitor(self):
        """Return carrier code of requested main competitor sent with parameter: 'main_competitor'"""
        self.main_competitor.data

    def get_point_of_sales_list(self):
        """Return list of country of sale codes sent with parameter: 'pos'"""
        return self.pos.data.split(",") if self.pos.data else Constants.ALL

    def get_selected_year_month(self):
        """Return year,month pair converted from 'selected_yearmonth' parameter.
        If year is not specified it returns None for year
        If month is not specified it returns None for month"""
        year = None
        month = None
        arr = self.selected_yearmonth.data.split("-")

        if len(arr) == 2:
            year, month = arr
            return year, month
        year, month, day = arr
        return year, month, day

    def get_pax_type(self):
        if self.pax_type.data == "IND":
            return False
        if self.pax_type.data == "GRP":
            return True
        return "All"

    def get_selected_holidays(self):
        if not self.selected_holidays.data or "All" in self.selected_holidays.data:
            return []
        return self.selected_holidays.data.split(",")

    @classmethod
    def get_date_object(self, dt: str) -> date:
        year, month, day = dt.split("-")
        return date(year=int(year), month=int(month), day=int(day))

    @classmethod
    def get_date_parts(self, dt: str) -> Tuple[int, ...]:
        parts = dt.split("-")
        if len(parts) == 2:
            year, month = parts
            return int(year), int(month)

        year, month, day = parts
        return int(year), int(month), int(day)


class MsdCarriers(BaseMSDFilterParams):
    pass


######### Product Overview UI page related forms #########


class MsdGetProductMap(BaseMSDFilterParams):
    pass


class MsdBaseProductOverviewForm(BaseMSDFilterParams):
    pass


class MsdGetDistrMix(MsdBaseProductOverviewForm):
    pass


class MsdGetCabinMix(MsdBaseProductOverviewForm):
    pass


class MsdGetBkgMix(MsdBaseProductOverviewForm):
    pass


class RangeSliderValues(FlaskForm):
    agg_type = StringField(
        "agg_type",
        [Optional()]
        + one_of(
            "agg_type",
            [
                Constants.AGG_VIEW_YEARLY,
                Constants.AGG_VIEW_MONTHLY,
                Constants.AGG_VIEW_DAILY,
            ],
        ),
    )

    time_direction = StringField(
        "time_direction",
        [Optional()]
        + one_of(
            "time_direction",
            [
                "forward",
                "historical",
                "historical-forward",
            ],
        ),
    )


class PosBreakDownTables(Form):
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])

    def comptitor(self):
        return self.comptitor.data


class MsdGetCosBreakdown(Form):
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])
    selected_yearmonth = StringField("selected_yearmonth", [Regexp("^\d{4}$")])
    pos = StringField("pos", [Optional()])
    selected_competitors = StringField(
        "selected_competitors",
        [Optional(), *comma_separated_characters_only("selected_competitors")],
    )
    main_competitor = StringField("main_competitor", [DataRequired()])

    def competitor(self):
        return self.main_competitor.data

    def year(self):
        return int(self.selected_yearmonth.data)

    def get_selected_competitors(self) -> List[str]:
        res = [*super().get_selected_competitors()]
        if self.main_competitor.data:
            res.append(self.main_competitor.data)
        return res
