from dataclasses import dataclass

from base.helpers.user import User
from fares.common.query import Match
from fares.keys.forms import GetFlightKeys
from fares.repository import FareRepository


@dataclass
class GroupedKeys:
    form: GetFlightKeys
    user: User

    @property
    def query(self):

        match = Match(
            user=self.user,
            origin=self.form.get_origin(user=self.user),
            destination=self.form.get_destination(user=self.user),
            graph_carriers=self.form.get_graph_carriers(),
            date_range=self.form.get_date_range() if not self.form.get_date() else None,
            time_range=self.form.get_time_range(),
            connections=self.form.get_connection(),
            weekdays=self.form.get_weekdays(),
            duration=self.form.get_duration(),
            flight_type=self.form.get_ftype(self.user.carrier),
            date=self.form.get_date(),
        ).query

        exp = FareRepository.create_expiry_fares_date_match()
        consider_exp_rules = bool(exp) and not self.form.get_date()

        pipelines = [
            {
                "$match": match,
            },
            {"$unwind": {"path": "$lowestFares"}},
            {
                "$addFields": {
                    "belongsToCabin": {
                        "$regexMatch": {
                            "input": "$lowestFares.cabinName",
                            "regex": self.form.get_cabin(normalize=False)[0],
                            "options": "i",
                        }
                    }
                }
            },
            {"$match": {"$or": FareRepository.create_expiry_fares_date_match()} if consider_exp_rules else {}},
            {"$match": {"belongsToCabin": True}},
            {"$addFields": {"outbound": {"$first": "$itineraries"}}},
            {"$addFields": {"outbound_leg": {"$first": "$outbound.legs"}}},
            {"$project": {"_id": 0, "carrierCode": 1, "fltNum": "$outbound_leg.opFltNum"}},
        ]

        return pipelines
