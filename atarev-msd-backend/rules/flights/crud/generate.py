from dataclasses import dataclass

import pandas as pd
from flask import Request

from airports.repository import AirportRepository
from base.helpers.user import User
from events.repository import EventRepository
from filters.repository import FilterRepository
from regions.repository import RegionRepository
from rules.flights.crud import CreateFlightRuleForm

event_repo = EventRepository()


@dataclass
class GenerateFlightRule:
    item: CreateFlightRuleForm
    user: User

    def generate(self):
        condition = {
            "all": [
                {
                    "value": self.item.origin,
                    "operator": "in",
                    "path": "market.originCityCode",
                    "fact": "market"
                },
                {
                    "value": self.item.destination,
                    "operator": "in",
                    "path": "market.destCityCode",
                    "fact": "market"
                },
                {
                    "value": self.item.cabin,
                    "operator": "equal",
                    "path": "cabin.cabinCode",
                    "fact": "cabin"
                },
                {
                    "field": "competitor_range",
                    "all": [
                        {
                            "value": -self.item.competitor_range[0],
                            "operator": "greaterThanInclusive",
                            "path": "competitor.departureTimeDifferenceInHoursMin",
                            "fact": "competitor"
                        },
                        {
                            "value": self.item.competitor_range[1],
                            "operator": "lessThanInclusive",
                            "path": "competitor.departureTimeDifferenceInHoursMax",
                            "fact": "competitor"
                        }
                    ]
                },
                {
                    "value": self.item.competitor,
                    "operator": "equal",
                    "path": "competitor.competitorCode",
                    "fact": "competitor"
                },
            ]
        }
        if self.item.effective_time:
            times = self.item.effective_time
            first = int(times[0].replace(":", ""))
            sec = int(times[1].replace(":", ""))
            condition["all"].append({
                "field": "effective_time",
                "all": [
                    {
                        "value": first,
                        "operator": "greaterThanInclusive",
                        "path": "leg.deptTime",
                        "fact": "leg"
                    },
                    {
                        "value": sec,
                        "operator": "lessThanInclusive",
                        "path": "leg.deptTime",
                        "fact": "leg"
                    }
                ]
            })
        if self.item.departure_date:
            condition["all"].append({
                "field": "departure_date",
                "all": [
                    {
                        "value": int(self.item.departure_date[0]),
                        "operator": "greaterThanInclusive",
                        "path": "leg.deptDate",
                        "fact": "leg"
                    },
                    {
                        "value": int(self.item.departure_date[1]),
                        "operator": "lessThanInclusive",
                        "path": "leg.deptDate",
                        "fact": "leg"
                    }
                ]
            },
            )
        if self.item.equipment:
            condition["all"].append({
                "value": self.item.equipment,
                "operator": "in",
                "path": "leg.equipmentCode",
                "fact": "leg"
            })
        if self.item.dow:
            condition["all"].append({
                "value": self.item.dow,
                "operator": "in",
                "path": "leg.dayOfWeek",
                "fact": "leg"
            })

        if self.item.dtd:
            condition["all"].append({
                "field": "dtd",
                "all": [
                    {
                        "value": self.item.dtd[0],
                        "operator": "greaterThanInclusive",
                        "path": "leg.daysToDeparture",
                        "fact": "leg"
                    },
                    {
                        "value": self.item.dtd[1],
                        "operator": "lessThanInclusive",
                        "path": "leg.daysToDeparture",
                        "fact": "leg"
                    }
                ]
            },
            )

        if self.item.analysis:
            v = self.item.analysis[0].apply.split("_")
            condition["all"].append({
                "field": "analysis",
                "all": [
                    {
                        "fact": v[0],
                        "path": f"{v[0]}.{v[1]}",
                        "operator": self.item.analysis[0].operator,
                        "value": self.item.analysis[0].value[0]
                    }
                ]
            })

        generated = {
            "ruleName": self.item.name,
            "ruleDescription": self.item.description,
            "ruleNote": self.item.note,
            "username": self.user.username,
            "carrier": self.user.carrier,
            "conditions": condition
        }

        if self.item.event:
            generated["event"] = self.item.event.dict()

        if self.item.effective_date:
            generated["effectiveDateStart"] = int(self.item.effective_date[0])
            generated["effectiveDateEnd"] = int(self.item.effective_date[1])

        if self.item.priority:
            generated["priority"] = self.item.priority

        if self.item.is_active:
            generated["isActive"] = self.item.is_active

        if self.item.is_auto:
            generated["isAuto"] = self.item.is_auto

        return generated


filter_repo = FilterRepository()
airport_repo = AirportRepository()
region_repo = RegionRepository()


@dataclass
class GenerateFlightRuleOption:
    request: Request

    def __get_city_airport_origins(self, market_df: pd.DataFrame):
        """get origin city codes to be considered in filters"""
        return market_df.market_origin_city_code.unique().tolist()

    def __get_city_airport_destinations(self, market_df: pd.DataFrame):
        """get destination city codes to be considered in filters"""
        return market_df.market_destination_city_code.unique().tolist()

    def __set_group(self, city_list):
        cities = list(
            airport_repo.get_airports_grouped_by_city({"city_code": {"$in": city_list}},
                                                      as_map=False))
        for city in cities:
            for airport in city["airports"]:
                airport["city_code"] = city["city_code"]
            city["code"] = city["city_code"]
            city["name"] = city["city_name"]
            del city["city_code"], city["city_name"]
        return cities

    def generate(self):
        filters_data = filter_repo.find_one({"customer": self.request.user.carrier})
        markets_df = pd.DataFrame(filters_data["markets"])
        origins = self.__get_city_airport_origins(markets_df)
        destinations = self.__get_city_airport_destinations(markets_df)
        origin_group = self.__set_group(origins)
        destination_group = self.__set_group(destinations)
        regions = region_repo.find({})

        eq = ['A21N', 'A319', 'A320', 'A321', 'A332', 'A343',
              'A388', 'AT43', 'AT72', 'B190', 'B737', 'B738',
              'B739', 'B743', 'B773', 'B777', 'B788', 'B789',
              'A20N', 'B78X', 'DASH8', 'E190', 'E195', 'E295',
              'E75S', 'F70', 'MD83', 'MD83', 'SAAB340', 'SF34']
        comps = list({competitor for f in filters_data["markets"]
                      for competitor in f["competitors"]})
        return {
            "cabins": [
                {
                    "label": "ECO",
                    "value": "ECONOMY"
                },
                {
                    "label": "BUS",
                    "value": "BUSINESS"
                },
                {
                    "label": "FIRST",
                    "value": "FIRST"
                }
            ],
            "actions": [
                {
                    "label": "UPSELL",
                    "value": "UPSELL",
                    "readable": "The strategy is to prevent the flight to be sold out at an unnecessarily low level.UPSELL Category picks flights with Variance MAF <b>LOWER</b> then the selected level. For example Variance MAF should never be lower than -50 USD, meaning you do not want to sell any seats more than 50 USD cheaper to your competitor</br>\n<b>Upsell Formula</b></br>\n<b>Variance MAF < -50</b></br>\nIn this case LFA will identify all the flights which are selling lower than the competitor by more than 50 USD and close the 1.class or the 2.class (based on your setup) and keep the upper class available "
                },
                {
                    "label": "DOWNSELL",
                    "value": "DOWNSELL",
                    "readable": "The strategy is to be as competitive as it can be.\nDOWNSELL Category picks flights with Variance MAF <b>HIGHER</b> than the selected level, for example Variance MAF should never be hight than 0- USD, meaning I do not want to be more expensive than my competitor</br>\n<b>DOWNSELL Formula</b> </br>\n<b>Variance MAF > 0</b></br>\nIn this case LFA will identify all the flights which are selling hight than the competitor by even more than 1 USD and opens up the availability by 1 or 2 class (based on your setup) "
                },
                {
                    "label": "UNDERCUT",
                    "value": "UNDERCUT",
                    "readable": ""
                },
                {
                    "label": "MATCH",
                    "value": "MATCH",
                    "readable": ""
                },
                {
                    "label": "NO ACTION",
                    "value": "NO ACTION",
                    "readable": ""
                }
            ],
            "ranks": [
                {
                    "label": "1. Closest Class",
                    "value": "CLOSEST_CLASS_1"
                },
                {
                    "label": "2. Closest Class",
                    "value": "CLOSEST_CLASS_2"
                },
                {
                    "label": "Continuous Fare",
                    "value": "CONTINUOUS_FARE"
                }
            ],
            "analysis": [
                {
                    "label": "Variance MAF",
                    "value": "fares_maf"
                }
            ],
            "origins": origin_group,
            "destinations": destination_group,
            "equipments": [{
                "value": e,
                "label": e
            } for e in eq],
            "dow": [
                {
                    "label": "M",
                    "value": 1
                },
                {
                    "label": "T",
                    "value": 2
                },
                {
                    "label": "W",
                    "value": 3
                },
                {
                    "label": "T",
                    "value": 4
                },
                {
                    "label": "F",
                    "value": 5
                },
                {
                    "label": "S",
                    "value": 6
                },
                {
                    "label": "S",
                    "value": 7
                }
            ],
            "operators": [
                {
                    "label": "=",
                    "value": "equal",
                    "readable": "Equal"
                },
                {
                    "label": "!=",
                    "value": "notEqual",
                    "readable": "Not Equal"
                },
                {
                    "label": ">=",
                    "value": "greaterThanInclusive",
                    "readable": "Greater or Equal"
                },
                {
                    "label": ">",
                    "value": "greaterThan",
                    "readable": "Greater Then"
                },
                {
                    "label": "<=",
                    "value": "lessThanInclusive",
                    "readable": "Less or Equal"
                },
                {
                    "label": "<",
                    "value": "lessThan",
                    "readable": "Less Then"
                },
                {
                    "label": "< >",
                    "value": "between",
                    "readable": "Between"
                }
            ],
            "regions": [{
                "label": r["region_name"],
                "value": r["region_code"]
            } for r in list(regions)],
            "competitors": [{
                "label": c,
                "value": c
            } for c in comps]

        }
