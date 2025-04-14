from flask import request

from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from network.forms import NetworkByondPoints, NetworkConictivityMap, NetworkSchedulingComparisonDetails
from network.service import NetworkService
from utils.funcs import create_error_response

service = NetworkService()


class NetworkController(BaseController):

    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_NETWORK_SCHEDULING_BEYOND_POINTS.value:
            return service.network_beyond_points(form)

        if endpoint == ProtectedRoutes.GET_NETWORK_SCHEDULING_COMPARISON_DETAILS.value:
            return service.network_comparison_details(form)

        if endpoint == ProtectedRoutes.GET_NETWORK_SCHEDULING_CONICTIVITY_MAP.value:
            return service.network_conictivity_map(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_NETWORK_SCHEDULING_BEYOND_POINTS.value:
            return NetworkByondPoints

        if endpoint == ProtectedRoutes.GET_NETWORK_SCHEDULING_COMPARISON_DETAILS.value:
            return NetworkSchedulingComparisonDetails

        if endpoint == ProtectedRoutes.GET_NETWORK_SCHEDULING_CONICTIVITY_MAP.value:
            return NetworkConictivityMap

        return super().get_validation_class()
