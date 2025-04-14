from flask import request

from base.controller import BaseController
from base.helpers.routes import FreeRoutes
from rules.flights.recommendations.form import RecommendationForm
from rules.routes import Route
from rules.service import RuleService

service = RuleService()


class RuleController(BaseController):

    def get(self, endpoint: str):
        form = super().get(endpoint)

        if endpoint == Route.RECOMMENDATIONS.value:
            return service.get_recommendations(form)

        if endpoint == Route.STORE_EVENT_RULES.value:
            return service.get_event_rules_options()

        if endpoint == Route.STORE_FLIGHT_RULES.value:
            return service.get_flight_rules_options()

    def post(self, endpoint: str):

        if endpoint == Route.STORE_EVENT_RULES.value:
            return service.create_event_rule(request.json or {})

        if endpoint == Route.STORE_FLIGHT_RULES.value:
            return service.create_flight_rule(request.json or {})

        if endpoint == FreeRoutes.EVALUATE_FLIGHTS.value:
            return service.evaluate_flight_rules(request.json or {})

        if endpoint == FreeRoutes.EVALUATE_ALERTS.value:
            return service.evaluate_event_rules(request.json or {})

        return

    def delete(self, endpoint: str):
        if endpoint == Route.STORE_EVENT_RULES.value:
            return service.delete_event_rule(request.json or {})

        if endpoint == Route.STORE_FLIGHT_RULES.value:
            return service.delete_flight_rule(request.json or {})

    def put(self, endpoint: str):
        if endpoint == Route.STORE_FLIGHT_RULES.value:
            return service.update_flight_rule(request.json or {})

        if endpoint == Route.STORE_EVENT_RULES.value:
            return service.update_event_rule(request.json or {})

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == Route.RECOMMENDATIONS.value:
            return RecommendationForm

        return super().get_validation_class()
