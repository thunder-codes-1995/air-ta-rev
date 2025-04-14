from flask import request

from base.constants import Constants
from base.controller import BaseController
from fares.availability_trends.forms import GetMinFareTrends
from fares.common.form import FareForm
from fares.forms import GetFareStructureTable
from fares.health.forms import TrackFares
from fares.keys.forms import GetFlightKeys
from fares.price_evoluation.forms import GetPriceEvolution
from fares.routes import Route
from fares.service import FareService
from fares.structure.form import GetFareStructure
from utils.funcs import create_error_response

fare_service = FareService()


class FareController(BaseController):

    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == Route.GET_FLIGHTS.value:
            return fare_service.get_flights(form)

        if endpoint == Route.GET_MIN_FARE_TRENDS.value:
            return fare_service.get_min_fare_trends(form)

        if endpoint == Route.GET_PRICE_EVOLUTION.value:
            return fare_service.get_price_evolution(form)

        if endpoint == Route.GET_SCRAPER_HEALTH.value:
            return fare_service.get_scraper_health(form)

        if endpoint == Route.GET_FARE_STRUCTURE.value:
            return fare_service.get_fare_structure(form)

        if endpoint == Route.GET_FARE_STRUCTURE_TABLE.value:
            return fare_service.get_fare_structure_table(form)

        if endpoint == Route.GET_MIN_FARE_TRENDS_TABLE.value:
            return fare_service.get_min_fare_trends_table(form)

        if endpoint == Route.GET_PRICE_EVOLUTION_TABLE.value:
            return fare_service.get_price_evolution_table(form)

        if endpoint == Route.GET_MIN_FARE_TRENDS_REPORT.value:
            return fare_service.get_min_fare_trends_report(form)

        if endpoint == Route.GET_PRICE_EVOLUTION_REPORT.value:
            return fare_service.get_price_evolution_report(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == Route.GET_MIN_FARE_TRENDS.value:
            return GetMinFareTrends

        if endpoint == Route.GET_PRICE_EVOLUTION.value:
            return GetPriceEvolution

        if endpoint == Route.GET_FARE_STRUCTURE_TABLE.value:
            return GetFareStructureTable

        if endpoint == Route.GET_FARE_STRUCTURE.value:
            return GetFareStructure

        if endpoint == Route.GET_SCRAPER_HEALTH.value:
            return TrackFares

        if endpoint == Route.GET_FLIGHTS.value:
            return GetFlightKeys

        if endpoint == Route.GET_MIN_FARE_TRENDS_TABLE.value:
            return GetMinFareTrends

        if endpoint == Route.GET_PRICE_EVOLUTION_TABLE.value:
            return GetPriceEvolution

        if endpoint == Route.GET_MIN_FARE_TRENDS_REPORT.value:
            return GetMinFareTrends

        if endpoint == Route.GET_PRICE_EVOLUTION_REPORT.value:
            return GetPriceEvolution

        return super().get_validation_class()
