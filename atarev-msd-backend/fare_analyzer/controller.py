from base.constants import Constants
from base.controller import BaseController
from fare_analyzer import service
from fare_analyzer.routes import Route
from utils.funcs import create_error_response


class FareAnalyzer(BaseController):
    def get(self, endpoint: str):

        if endpoint == Route.GET_FARES.value:
            return service.FareAnalyzerService().get_all_fares()

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)
