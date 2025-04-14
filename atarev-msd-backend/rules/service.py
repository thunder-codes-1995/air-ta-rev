from typing import Dict

from flask import request

from base.service import BaseService
from rules.events.crud import EventRule
from rules.events.crud.forms import CreateEventRuleForm
from rules.events.evaluation.form import AlertEvaluationForm
from rules.events.evaluation.process import Proccess as ProccessEventRules
from rules.flights.crud import FlightRule
from rules.flights.crud.forms import CreateFlightRuleForm

from rules.flights.evaluation.form import RuleEvaluationForm
from rules.flights.evaluation.process import Proccess as ProccessFlightRules
from rules.flights.recommendations import RecommendationTable
from rules.flights.recommendations.form import RecommendationForm



class RuleService(BaseService):
    def evaluate_flight_rules(self, payload: Dict[str, str]):
        form = RuleEvaluationForm(**payload)
        return ProccessFlightRules(form).evaluate()

    def get_recommendations(self, form: RecommendationForm):
        return RecommendationTable(form, request.user.carrier).get()

    def evaluate_event_rules(self, payload: Dict[str, str]):
        form = AlertEvaluationForm(**payload)
        return ProccessEventRules(form).evaluate()

    def get_event_rules_options(self):
        return EventRule(request).options()

    def get_flight_rules_options(self):
        return FlightRule(request).options()

    def create_flight_rule(self, payload: Dict):
        form = CreateFlightRuleForm(**payload)
        return FlightRule(request, form).create()

    def update_flight_rule(self, payload: Dict):
        form = CreateFlightRuleForm(**payload)
        return FlightRule(request, form).update()

    def delete_flight_rule(self, payload: Dict):
        return FlightRule(request).delete(payload.get('id'))

    def create_event_rule(self, payload: Dict):
        form = CreateEventRuleForm(**payload)
        return EventRule(request, form).create()

    def update_event_rule(self, payload: Dict):
        form = CreateEventRuleForm(**payload)
        return EventRule(request, form).update()

    def delete_event_rule(self, payload: Dict):
        return EventRule(request).delete(payload.get('id'))
