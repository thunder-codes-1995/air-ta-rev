import calendar
from dataclasses import dataclass
from typing import List, Literal, TypedDict, Union

from base.helpers.datetime import Date, Time
from rules.flights.recommendations.data import Hover, HoverDataMap
from rules.flights.recommendations.form import RecommendationForm
from rules.flights.recommendations.query import RecommendationsQuery
from rules.repository import RuleResultRepository
from rules.types import RuleResultItem

rule_result_repo = RuleResultRepository()


class Reco(TypedDict):
    action = str
    actionArrow = str
    hostFlight = str
    hostFlightOD = str
    hostFlightDepartureDate = str
    hostFlightDepartureTime = str
    hostFlightClass = (Union[None, str],)
    hostFare = float
    mainCompetitorFare = float
    mainCompetitorDepartureTime = str
    lowestFare = float
    lowestDepartureTime = (str,)
    maf = float
    daysToDeparture = int
    dayOfWeek = int
    dayOfWeekString = (str,)
    cabinCode = str
    ruleName = str
    createdAt = str


class RecommendationResp(Reco):
    hoverText: Hover


@dataclass
class Recommendation:
    rule_result: RuleResultItem

    def get(self) -> Reco:
        return {
            "action": self.rule_result["action"]["type"],
            "actionArrow": self.arrow(),
            "hoverText": "",
            "hostFlight": f"{self.rule_result['facts']['leg']['carrierCode']}{self.rule_result['facts']['leg']['flightNumber']}",
            "hostFlightOD": f"{self.rule_result['facts']['leg']['originCode']}{self.rule_result['facts']['leg']['destCode']}",
            "hostFlightDepartureDate": Date(self.rule_result["facts"]["leg"]["deptDate"]).humanize(),
            "hostFlightDepartureTime": Time(self.rule_result["facts"]["leg"]["deptTime"]).humanize(),
            "hostFlightClass": self.rule_result["facts"]["hostFare"]["classCode"],
            "hostFare": self.rule_result["facts"]["hostFare"]["fareAmount"],
            "mainCompetitorFare": self.rule_result["facts"]["mainCompetitorFare"]["fareAmount"],
            "mainCompetitorDepartureTime": Time(self.rule_result["facts"]["mainCompetitorFare"]["deptTime"]).humanize(),
            "lowestFare": self.rule_result["facts"]["lowestFare"]["fareAmount"],
            "lowestDepartureTime": Time(self.rule_result["facts"]["lowestFare"]["deptTime"]).humanize(),
            "maf": self.rule_result["facts"]["fares"]["maf"],
            "daysToDeparture": self.rule_result["facts"]["leg"]["daysToDeparture"],
            "dayOfWeek": self.rule_result["facts"]["leg"]["dayOfWeek"],
            "dayOfWeekString": calendar.day_abbr[self.rule_result["facts"]["leg"]["dayOfWeek"]],
            "cabinCode": self.rule_result["facts"]["cabin"]["cabinCode"],
            "ruleName": self.rule_result["ruleName"],
            "createdAt": self.creation(self.rule_result["created_at"]),
            # 'effectiveAvailability' : '',
            # 'effectiveFare' : '',
            # "effectiveClass" :
        }

    def creation(self, value: int) -> str:
        string_val = f"{value}"
        dt = Date(int(string_val[0:8])).humanize()
        time = Time(int(string_val[8:12])).humanize()
        return f"{dt} {time}"

    def arrow(self) -> Literal["HORIZONTAL", "UP", "DOWN", "NONE"]:
        if self.rule_result["action"]["type"] == "UPSELL":
            return "UP"
        if self.rule_result["action"]["type"] in ["DOWNSELL", "UNDERCUT"]:
            return "DOWN"
        if self.rule_result["action"]["type"] == "MATCH":
            return "HORIZONTAL"
        return "NONE"


@dataclass
class RecommendationTable:
    form: RecommendationForm
    host_code: str

    def get(self) -> List[RecommendationResp]:
        res = []
        hover_map = HoverDataMap(self.host_code).get()
        match = RecommendationsQuery(self.host_code, self.form).query

        for item in rule_result_repo.find(match):
            obj = Recommendation(item).get()
            obj["hoverText"] = hover_map.get(item["ruleId"], {})
            res.append(obj)

        return res
