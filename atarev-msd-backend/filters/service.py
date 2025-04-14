from typing import Iterable, List, Optional

import pandas as pd
from flask import request

from airports.repository import AirportRepository
from base.entities.carrier import Carrier
from base.entities.country_city import CountryCity
from base.helpers.user import User
from base.service import BaseService
from configurations.repository import ConfigurationRepository
from fares.repository import FareRepository
from filters.forms import FilterOptionsForm, GetCustomerMarketsForm
from filters.options import CategoryOption
from filters.options.types import Category, SimpleOnD
from filters.repository import FilterRepository
from regions.repository import RegionRepository
from users.repository import UserRepository

filter_repo = FilterRepository()
airport_repo = AirportRepository()
region_repo = RegionRepository()
config_repo = ConfigurationRepository()
user_repo = UserRepository()
fare_repo = FareRepository()


class EVENT_ANALYZER_FILTER_OPTIONS(SimpleOnD):
    categories: List[Category]


class MarketService(BaseService):

    def get_events_analyzer_filter_options(self, form: FilterOptionsForm) -> EVENT_ANALYZER_FILTER_OPTIONS:

        if form.origin_city.data:
            form.country_city_lookup.data = None

        if not form.origin_city.data and not form.destination.data:
            c = config_repo.get_user_default_settings(request.user.carrier)
            form.origin_city.data = ",".join(c["value"].get("origCountryCity"))
            form.destination.data = ",".join(c["value"].get("destCountryCity"))

        co = CategoryOption(user=request.user, form=form)
        if request.method == "POST":
            body = request.json
            co.event_ids = body.get("event_ids")
            co.group_ids = body.get("group_ids")
            return {"message": co.post()}

        return {
            **CountryCity(host_code=request.user.carrier, origin=form.origin_city.data, lookup=form.country_city_lookup.data).get(form.origin_city.data),
            "categories": co.get(),
        }

    def get_filter_options(self, form: FilterOptionsForm):
        if form.target.data == "ea":
            return self.get_events_analyzer_filter_options(form)

        return self.global_filter_options(request.user)

    def check_rt_and_ow_exist(self, customer, orig, dest) -> List[str]:
        origins = orig.split(",")
        destinations = dest.split(",")
        markets = config_repo.get_by_key("MARKETS", customer)

        # handle city-based markets
        city_market = Carrier(customer).bridge(origins, destinations).get_city_based_makret()
        f = filter(lambda item: item["orig"] == city_market.origin and item["dest"] == city_market.destination, markets)
        res = [market["direction"] for market in f]

        # handle airport-based markets
        if not res:
            f = filter(lambda item: item["orig"] in origins and item["dest"] in destinations, markets)
            res = [market["direction"] for market in f]

        return list(set(res)) or ["OW"]

    def global_filter_options(self, user: User):
        """get filters for msd"""
        # get all available markets for host carrier
        filters_data = filter_repo.find_one({"customer": request.user.carrier})
        markets_df = pd.DataFrame(filters_data["markets"])
        origins = self.__get_city_airport_origins(markets_df)
        destinations = self.__get_city_airport_destinations(markets_df)
        _filter = self.__get_filters(markets_df, origins, destinations, user)

        if request.args.get("orig_city_airport") and request.args.get("dest_city_airport", ""):
            self.__store_selected_filter_options()
            flight_type = self.check_rt_and_ow_exist(
                request.user.carrier,
                request.args.get("orig_city_airport"),
                request.args.get("dest_city_airport"),
            )

            if len(flight_type) == 2:
                _filter["flight_type_switch"] = True
                _filter["all_flight_type"] = None

            else:
                _filter["flight_type_switch"] = False
                _filter["all_flight_type"] = flight_type[0]

        return _filter

    def __can_store(self, data) -> bool:
        """check if any of selected values is not valid"""
        origin = self.split_string(request.args.get("orig_city_airport", ""), allow_empty=False)
        destination = self.split_string(request.args.get("dest_city_airport", ""), allow_empty=False)
        origin_city_map = airport_repo.get_airports_grouped_by_city({"airport_iata_code": {"$in": origin}})
        destination_city_map = airport_repo.get_airports_grouped_by_city({"airport_iata_code": {"$in": destination}})

        # if user selected airports that belong to many cities -> don't store
        if len(origin_city_map.keys()) != 1 or len(destination_city_map.keys()) != 1:
            return False

        origin_city = list(origin_city_map.keys())[0]
        destination_city = list(destination_city_map.keys())[0]
        market = filter_repo.get_market(origin_city, destination_city, request.user.carrier)

        # if market is invalid -> don't store
        if not market:
            return False

        # if main competitor does not belont to market -> don't store
        if not data.get("main_competitor") or data.get("main_competitor")[0] not in market["competitors"]:
            return False

        # if any of other_competitors does not belong to selected market -> don't store
        if any(
            obj not in market["competitors"]
            for obj in filter(lambda comp: bool(comp) and comp != "All", data.get("selected_competitors", []))
        ):
            return False

        return True

    def __store_selected_filter_options(self):
        fields = [
            "main_competitor",
            "selected_competitors",
            "orig_city_airport",
            "dest_city_airport",
            "orig_region",
            "dest_region",
            "orig_country",
            "dest_country",
            "pos",
            "sales_channel",
            "cabin",
            "pax_type",
        ]

        _filter = {"selected_competitors": []}

        for field in fields:
            val = self.split_string(request.args.get(field, ""), allow_empty=False)

            # if user selected (only) "All" or user selected many values and "All" is not one of them -> store value
            if len(val) == 1 and val[0].lower() == "all" or all(v and v.lower() != "all" for v in val):
                _filter[field] = self.split_string(request.args.get(field, ""), allow_empty=False)

            # 2 - if user selected many values and "All" is one of them store only "All" and ignore the rest
            if not all(v and v.lower() != "all" for v in val):
                _filter[field] = ["All"]

        if not _filter or not self.__can_store(_filter):
            return

        user_repo.store_filter_options(request.user.username, **_filter)

    def __get_city_airport_origins(self, market_df: pd.DataFrame):
        """get orgiin city codes to be considered in filters"""
        return market_df.market_origin_city_code.unique().tolist()

    def __get_city_airport_destinations(self, market_df: pd.DataFrame):
        """get destination city codes to be considered in filters"""
        origin = request.args.get("orig_city_airport")
        if not origin:
            return market_df.market_destination_city_code.unique().tolist()

        # i need city code to search markets
        origin = self.split_string(origin)
        origin = self.__get_city(origin)
        if not origin:
            return market_df.market_destination_city_code.unique().tolist()

        filtered_df = market_df[market_df.market_origin_city_code == origin]
        return filtered_df.market_destination_city_code.unique().tolist()

    def __get_market_options(
        self,
        regions_df: pd.DataFrame,
        codes: List[str],
        loockup_key: str,
        lookup_group: Iterable[str],
        user: User,
    ):
        """get both orig_city_code and dest_city_code to be sent to ui"""
        options = []
        filtered = regions_df[regions_df["city_code"].isin(codes)]
        filtered = self.__filter_by_lookup(filtered, loockup_key)
        user_assigned_markets = user.markets

        # below operations could've been accomplished using groupby
        # the problem with groupby is it does not preserve the order of cities
        # i need to get options in certain order (looked for options then selected options)
        unique_cities_df = filtered.drop_duplicates("city_code")[["city_code", "city_name"]]
        for _, row in unique_cities_df.iterrows():
            city_code, city_name = row
            airports_df = filtered[filtered.city_code == city_code][["airport_name", "airport_code", "city_code"]]

            if lookup_group:
                airports_df = airports_df[airports_df.airport_code.isin(lookup_group)]

            if user_assigned_markets and loockup_key.startswith("orig"):
                airports_df = airports_df[airports_df.airport_code.isin([m[0] for m in user_assigned_markets])]

            if user_assigned_markets and loockup_key.startswith("dest"):
                airports_df = airports_df[airports_df.airport_code.isin([m[1] for m in user_assigned_markets])]

            if airports_df.empty:
                continue

            options.append({"code": city_code, "name": city_name, "airports": airports_df.to_dict("records")})

        return options

    def __filter_by_lookup(self, df: pd.DataFrame, loockup_key):
        """if user is searching by keyword"""
        lookup = request.args.get(loockup_key, "").strip()
        if not lookup:
            return df

        selected = request.args.get(loockup_key.replace("_lookup", ""), "").strip().split(",")
        lookup = lookup.lower()

        lookup_df = df[
            (df.city_code.str.lower().str.match(f"^{lookup}.*")) | (df.airport_code.str.lower().str.match(f"^{lookup}.*"))
        ].sort_values(["city_code", "airport_code"])

        selected_df = df[df.airport_code.isin(selected)].sort_values("airport_code")
        return pd.concat([lookup_df, selected_df]).drop_duplicates(["city_code", "airport_code"])

    def __get_targeted_market(self, market_df: pd.DataFrame, origins: List[str], destinations: List[str]) -> pd.DataFrame:
        """get targeted market (both origin and destination shoulde be provided"""
        if not origins or not destinations:
            return
        city_origin = self.__get_city(origins)
        city_destination = self.__get_city(destinations)
        if not city_origin or not city_destination:
            return
        filtered = market_df[
            (market_df.market_origin_city_code == city_origin) & (market_df.market_destination_city_code == city_destination)
        ]
        filtered = filtered.dropna(axis=1)
        if filtered.empty:
            return
        return {"points_of_sale": [], "sales_channels": [], **filtered.iloc[0].to_dict()}

    def __get_filters(self, markets_df: pd.DataFrame, market_origins: List[str], market_destinations: List[str], user: User):
        all_cities = list(set(market_origins + market_destinations))
        selected_origins = self.split_string(request.args.get("orig_city_airport", ""), allow_empty=False)
        selected_destinations = self.split_string(request.args.get("dest_city_airport", ""), allow_empty=False)
        df = self.lambda_df(list(region_repo.get_region_by_cities(all_cities)))
        df = df[df.city_code.isin(all_cities)]
        # places collection's data has been uploaded with type = 'METROPOLITAN"
        # and i need to remove all records with that type (to avoid duplication) in airports
        df.drop_duplicates(["city_code", "airport_code"], inplace=True)
        regions = df.region_code.unique().tolist()
        countries = df.country_code.unique().tolist()
        selected_market = self.__get_targeted_market(markets_df, selected_origins, selected_destinations)
        competitors = self.__get_competitors(selected_market)
        user_market = request.user.markets

        return {
            "dest_region": regions,
            "orig_region": regions,
            "cabin": ["All", "BUS", "ECO"],
            "pax_type": ["All", "GRP", "IND"],
            "dest_country": countries,
            "orig_country": countries,
            "orig_city_airport": self.__get_market_options(
                df,
                market_origins,
                "orig_city_airport_lookup",
                tuple(market[0] for market in user_market),
                user,
            ),
            "dest_city_airport": self.__get_market_options(
                df,
                market_destinations,
                "dest_city_airport_lookup",
                tuple(market[1] for market in user_market),
                user,
            ),
            "main_competitor": competitors if competitors else [],
            "selected_competitors": ["All"] + competitors[1:] if competitors and len(competitors) > 1 else ["All"],
            "pos": ["All"] + selected_market["points_of_sale"] if selected_market else ["All"],
            "sales_channel": ["All"] + selected_market["sales_channels"] if selected_market else ["All"],
            "carriers": self.__get_carriers_color_data(selected_market),
            "currencies": config_repo.get_supported_currencies(),
        }

    def __get_carriers_color_data(self, market):
        mp = {}
        if not market:
            return mp
        for k, v in self.get_carrier_color_map().items():
            mp[k] = {"color": v, "name": k}
        return mp

    def __get_city(self, code_list: List[str]):
        """
        selelected origin can be either :
        - one city
        - multiple airports (belong to same city)
        """
        _map = airport_repo.get_airports_grouped_by_city({"airport_iata_code": {"$in": code_list}})
        cities = list(_map.keys())
        assert len(cities) != 0, "Invalid airport code"
        assert len(cities) == 1, "User can select only 1 city or multiple airports belong to same city"

        return cities[0]

    def __get_competitors(self, market):
        # if not market:
        # get user last selected filters
        user = user_repo.find_one({"username": request.user.username})
        selected_filter_options = user.get("selected_filter_options", {})
        user_selected_main_competitor = selected_filter_options.get("main_competitor", [])
        user_selected_other_competitors = selected_filter_options.get("selected_competitors", [])

        if not market:
            # if both main_competitor and other_competitors are already store in user_selected options -> no need to check default options
            # if other_competitors is not stoted and user has selected options (it means user selected empty value for other competitors deliberately)
            if (user_selected_main_competitor and user_selected_other_competitors) or (
                user_selected_main_competitor and user.get("selected_filter_options") and not user_selected_other_competitors
            ):
                return list(set([*user_selected_main_competitor, *user_selected_other_competitors]))

            return self.__handle_default_competitors()
        return self.__handle_market_competitors(market)

    def __handle_default_competitors(self):
        """get competitor options based on default filter values"""
        default_settings = config_repo.get_user_default_settings(request.user.carrier) or {"value": {}}

        default_other_competitors = default_settings["value"].get("selectedCompetitors", [])
        default_main_competitor = default_settings["value"].get("mainCompetitor", [])
        default_competitors = [*default_main_competitor, *default_other_competitors]
        return list(set(default_competitors))

    def __handle_market_competitors(self, market):
        """return comptitor options in case a market was selected"""
        main_competitor = request.args.get("main_competitor")
        if main_competitor and main_competitor in market["competitors"]:
            return [main_competitor, *list(filter(lambda comp: comp != main_competitor, market["competitors"]))]
        return market["competitors"]

    def get_customer_markets(self, form: GetCustomerMarketsForm):
        customer: str = form.customer.data
        markets = filter_repo.get_customer_markets(customer)
        table_labels = [{"orig": "Origin"}, {"dest": "Destination"}]
        table_data = []

        for market in markets:
            table_data.append({"orig": market["orig"], "dest": market["dest"]})

        return {"table": {"data": table_data, "labels": table_labels}}
