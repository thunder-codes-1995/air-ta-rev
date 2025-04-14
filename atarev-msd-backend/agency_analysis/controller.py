from flask import request

from agency_analysis.forms import AgencyGraph, AgencyQuadrant, AgencyTable
from agency_analysis.service import AgencyService
from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from utils.funcs import create_error_response

service = AgencyService()


class AgencyController(BaseController):

    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_AGENCY_TABLE.value:
            return service.get_agency_table(form)

        if endpoint == ProtectedRoutes.GET_AGENCY_QUADRANT.value:
            return service.get_agency_quadrant(form)

        if endpoint == ProtectedRoutes.GET_AGENCY_GRAPHS.value:
            return service.get_agency_graphs(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):

        endpoint = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_AGENCY_TABLE.value:
            return AgencyTable

        if endpoint == ProtectedRoutes.GET_AGENCY_QUADRANT.value:
            return AgencyQuadrant

        if endpoint == ProtectedRoutes.GET_AGENCY_GRAPHS.value:
            return AgencyGraph

        return super().get_validation_class()
