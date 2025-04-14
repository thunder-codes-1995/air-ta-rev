from flask import request, make_response, jsonify

from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from filters.forms import FilterOptionsForm, GetCustomerMarketsForm, MarketFilterOptions
from filters.service import MarketService
from utils.funcs import create_error_response

market_service = MarketService()


class FilterController(BaseController):

    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_FILTER_OPTIONS.value:
            return market_service.get_filter_options(form)

        if endpoint == ProtectedRoutes.GET_CUSTOMER_MARKETS.value:
            return market_service.get_customer_markets(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 404)

    def _build_cors_preflight_response(self):
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add('Access-Control-Allow-Headers', "*")
        response.headers.add('Access-Control-Allow-Methods', "*")
        return response

    def options(self, endpoint: str):
        return self._build_cors_preflight_response()


    def post(self, endpoint: str):
        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_FILTER_OPTIONS.value:
            response = make_response(jsonify(market_service.get_filter_options(form)))
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_MARKET_OPTIONS_BY_KEYWORD.value:
            return MarketFilterOptions

        if endpoint == ProtectedRoutes.GET_CUSTOMER_MARKETS.value:
            return GetCustomerMarketsForm

        if endpoint == ProtectedRoutes.GET_FILTER_OPTIONS.value:
            return FilterOptionsForm

        return super().get_validation_class()
