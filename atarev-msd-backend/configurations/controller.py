from flask import request

from base.constants import Constants
from base.controller import BaseController
from configurations.forms import GetConfigurationByKey, GetCustomerMarketsForm
from configurations.service import ConfigurationService
from utils.funcs import create_error_response

service = ConfigurationService


class ConfigController(BaseController):

    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == self.routes.GET_CONFIGURATION:
            return service.get_by_key(request.args.get("key"))

        if endpoint == self.routes.GET_CUSTOMERS:
            return service.get_customers()

        if endpoint == self.routes.GET_CUSTOMER_MARKETS:
            return service.get_customer_markets(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def put(self):
        if "competitors" or "currency" in request.json:
            origin = request.json.get("origin")
            destination = request.json.get("destination")

            if "competitors" in request.json:
                competitors = request.json.get("competitors")
                self.repository.update_market_cometitors(f"{origin}-{destination}", competitors, request.user.carrier)
            else:
                currency = request.json.get("currency")
                self.repository.update_market_currency(f"{origin}-{destination}", currency, request.user.carrier)

            return "Done !"

        self.repository.update(request.json, request.user.carrier)
        return "Done !"

    def get_validation_class(self):
        view_type = self.get_view_parameter("type")

        if view_type == self.routes.GET_CONFIGURATION:
            return GetConfigurationByKey

        if view_type == self.routes.GET_CUSTOMER_MARKETS:
            return GetCustomerMarketsForm

        return super().get_validation_class()
