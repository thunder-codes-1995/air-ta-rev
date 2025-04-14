import re
from typing import List
from typing import Optional as Op
from typing import Tuple, Union

from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired, Optional

from base.constants import Constants
from base.entities.exchange_rate import ExchangeRate
from base.helpers.cabin import CabinMapper
from base.helpers.theme import Theme
from base.helpers.user import User
from utils.rules import comma_separated_characters_only, date_range_rule, one_of


class Form(FlaskForm):
    dark_theme = StringField("dark_theme", [Optional()])
    currency = StringField("currency", [Optional()])

    def list_of_string(self, field_name: str) -> List[str]:
        field: StringField = getattr(self, field_name)
        if not field.data or not field.data.strip() or "all" in field.data.lower():
            return []
        return list(filter(lambda val: bool(val.strip()), field.data.split(",")))

    @classmethod
    def should_skip(cls, val):
        string_value = f"{val}".strip()

        if not val:
            return True

        if not string_value:
            return True

        pattern = re.compile(r"all", re.IGNORECASE)
        return bool(pattern.search(val))

    def is_country_market(self, fields=["orig_city_airport", "dest_city_airport"]) -> bool:
        origin = self.get_origin(fields[0])[0]
        destination = self.get_destination(fields[1])[0]
        return len(origin) == 2 and len(destination) == 2

    def get_origin(self, field="orig_city_airport", user: Op[User] = None) -> List[str]:
        if not self[field].data:
            return []

        if user:
            assigned_to_user = user.markets
            if assigned_to_user:
                return [code for code in self.list_of_string(field) if code in [m[0] for m in assigned_to_user]] or ["invalid"]

        return self.list_of_string(field)

    def get_destination(self, field="dest_city_airport", user: Op[User] = None) -> List[str]:
        if not self[field].data:
            return []

        if user:
            assigned_to_user = user.markets
            if assigned_to_user:
                return [code for code in self.list_of_string(field) if code in [m[1] for m in assigned_to_user]] or ["invalid"]

        return self.list_of_string(field)

    def get_graph_carriers(self) -> List[str]:
        if not self.graph_carriers.data:
            return [request.user.carrier]
        return list(set([request.user.carrier, *self.graph_carriers.data.split(",")]))

    def get_cabin(self, normalize: bool = True) -> List[str]:
        if self.should_skip(self.cabin.data or ""):
            return []

        if normalize:
            return [CabinMapper.normalize(c) for c in self.cabin.data.split(",")]

        return self.cabin.data.strip().split(",")

    def get_theme(self) -> Theme:
        if self.dark_theme.data == "true":
            return Theme.MID
        return Theme.LIGHT

    def get_currency(self) -> Union[str, None]:
        if not hasattr(self, "currency"):
            return
        return self.currency.data

    def get_currency_exchange_rate(self, base_currency: str = "USD", convert_to: str = None) -> float:
        convert_to_currency = convert_to or self.get_currency()
        if not convert_to_currency:
            return 1

        return ExchangeRate(base_currency, [convert_to_currency]).get_single_base_rates(base_currency)[convert_to_currency]

    def get_selected_competitors(self) -> List[str]:
        if not self.selected_competitors.data or "all" in self.selected_competitors.data.lower():
            return []
        return self.selected_competitors.data.split(",")

    def get_date_range(self) -> Tuple[int, int]:
        if not self.date_range_start.data or not self.date_range_end.data:
            return tuple()
        return (int(self.date_range_start.data.replace("-", "")), int(self.date_range_end.data.replace("-", "")))


from filters.service import MarketService


class BaseParams(Form):
    # getting filter_option
    market_service = MarketService()

    # Required fields
    orig_region = StringField("orig_region", [])
    dest_region = StringField("dest_region", [])
    orig_country = StringField("orig_country", [])
    dest_country = StringField("dest_country", [])
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])

    # Optional fields
    pos = StringField("pos", [Optional()])
    selected_competitors = StringField(
        "selected_competitors",
        [Optional(), *comma_separated_characters_only("selected_competitors")],
    )
    main_competitor = StringField("main_competitor", [Optional()])
    cabin = StringField("cabin", [*comma_separated_characters_only("cabin"), Optional()])
    sales_channel = StringField("sales_channel", [Optional()])
    selected_yearmonth = StringField(
        "selected_yearmonth",
        [Optional()] + date_range_rule("selected_yearmonth", day=False),
    )
    agg_view = StringField(
        "agg_view",
        [Optional()] + one_of("agg_view", [Constants.AGG_VIEW_YEARLY, Constants.AGG_VIEW_MONTHLY]),
    )
    dark_theme = StringField("dark_theme", [Optional()])
    currency = StringField("currency", [Optional()])

    # Below are helper functions to get data from request but already in a format that can be easily used in mongo queries
    # For example:
    #   "orig_city_airport" parameter can be comma separated string with airport codes
    #           so instead of parsing it and converting to array each time in multiple places, helper does it for you
    #   "date_range_start" is a date parameter and could already be converted once

    def get_currency(self) -> Union[str, None]:
        if not hasattr(self, "currency"):
            return
        return self.currency.data

    def get_currency_exchange_rate(self, base_currency: str = "USD", convert_to: str = None) -> float:
        convert_to_currency = convert_to or self.get_currency()
        if not convert_to_currency:
            return 1

        return ExchangeRate(base_currency, [convert_to_currency]).get_single_base_rates(base_currency)[convert_to_currency]

    def get_orig_city_airports_list(self):
        """Return list of airport codes sent with parameter: 'orig_city_airport'"""
        return [] if self.orig_city_airport.data is None else self.orig_city_airport.data.split(",")

    def get_orig_country_list(self):
        """Return list of country codes sent with parameter: 'orig_country'"""
        return [] if self.orig_country.data is None else self.orig_country.data.split(",")

    def get_orig_regions_list(self):
        """Return list of region codes sent with parameter: 'orig_region'"""
        return [] if self.orig_region.data is None else self.orig_region.data.split(",")

    def get_dest_city_airports_list(self):
        """Return list of airport codes sent with parameter: 'dest_city_airport'"""
        return [] if self.dest_city_airport.data is None else self.dest_city_airport.data.split(",")

    def get_graph_carriers(self):
        """Return list of airport codes sent with parameter: 'graph_carriers'"""
        if self.graph_carriers.data:
            return [request.user.carrier, *self.graph_carriers.data.split(",")]
        else:
            return [request.user.carrier]

    def get_dest_country_list(self):
        """Return list of country codes sent with parameter: 'dest_country'"""
        return [] if self.dest_country.data is None else self.dest_country.data.split(",")

    def get_dest_regions_list(self):
        """Return list of airport codes sent with parameter: 'dest_region'"""
        return [] if self.dest_region.data is None else self.dest_region.data.split(",")

    def get_cabins_list(self):
        """Return list of cabins codes sent with parameter: 'cabin'"""
        if not self.cabin.data:
            return []
        return self.cabin.data.split(",")

    def get_class_list(self):
        """Return list of classes sent with parameter: 'cabin' but mapped to class names
        Example: 'cabin' parameter was ECO, 'class' should be 'Discount Economy Class'
        """
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

    def get_selected_competitors_list(self):
        """
        Return list of carrier codes sent with parameters:
        'selected_competitors' and 'main_competitor' combined together
        """
        selected_competitors = []
        if not self.selected_competitors.data and not self.main_competitor.data:
            return []

        if self.main_competitor.data:
            selected_competitors.append(self.main_competitor.data)

        if self.selected_competitors.data:
            if Constants.ALL in self.selected_competitors.data:
                return self.market_service.get_filter_options()["main_competitor"]
            return [*selected_competitors, *self.selected_competitors.data.split(",")]
        return selected_competitors

    def get_start_date(self, as_int: bool = False):
        """
        returns start date value as int or str if exists
        otherwise returns None
        """

        if as_int and self.date_range_start.data:
            parts = self.date_range_start.data.split("-")
            assert len(parts) == 3, "start date format should be like : YYYY-MM-DD"
            return int(f"{parts[0]}{parts[1]}{parts[2]}")

        if not as_int and self.date_range_start.data:
            return self.date_range_start.data

        return None

    def get_end_date(self, as_int: bool = False):
        """
        returns end date value as int or str if exists
        otherwise returns None
        """
        if as_int and self.date_range_end.data:
            parts = self.date_range_end.data.split("-")
            assert len(parts) == 3, "end date format should be like : YYYY-MM-DD"
            return int(f"{parts[0]}{parts[1]}{parts[2]}")

        if not as_int and self.date_range_end.data:
            return self.date_range_end.data

        return None

    def get_main_competitor(self):
        """Return carrier code of requested main competitor sent with parameter: 'main_competitor'"""
        return self.main_competitor.data

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

    def get_is_dark_theme(self):
        if self.dark_theme.data == "true":
            return True
        return False

    def get_theme(self) -> Theme:
        if self.dark_theme.data == "true":
            return Theme.MID
        return Theme.LIGHT
