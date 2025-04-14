from flask import request

from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from base.middlewares import attach_carriers_colors
from market_share.forms import MarketShareFareAvg, MarketShareTrends
from market_share.service import MarketShareService
from utils.funcs import create_error_response

service = MarketShareService()


class MarketShareController(BaseController):

    @attach_carriers_colors()
    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_MARKET_SHARE_TRENDS.value:
            return service.get_market_share(form, Constants.MARKET_SHARE_TRENDS)

        if endpoint == ProtectedRoutes.GET_MARKET_SHARE_AVG_FARE.value:
            return service.get_market_share(form, Constants.MARKET_SHARE_AVG)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_MARKET_SHARE_TRENDS.value:
            return MarketShareTrends

        if endpoint == ProtectedRoutes.GET_MARKET_SHARE_AVG_FARE.value:
            return MarketShareFareAvg

        return super().get_validation_class()
