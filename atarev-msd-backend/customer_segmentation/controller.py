from flask import request

from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from customer_segmentation.forms import CustomerSegmentationGraphs, CustomerSegmentationTable
from customer_segmentation.service import CustomerSegmentationService
from utils.funcs import create_error_response

service = CustomerSegmentationService()


class CustomerSegmentationController(BaseController):

    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_CUSTOMER_SEGMENTATION_TABLE.value:
            return service.get_segmention_table(form)

        if endpoint == ProtectedRoutes.GET_CUSTOMER_SEGMENTION_GRAPHS.value:
            return service.get_segmention_graphs(form)

        if endpoint == ProtectedRoutes.GET_CUSTOMER_SEGMENTION_TEXT.value:
            return service.get_segmention_text()

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_CUSTOMER_SEGMENTATION_TABLE.value:
            return CustomerSegmentationTable

        if endpoint == ProtectedRoutes.GET_CUSTOMER_SEGMENTION_GRAPHS.value:
            return CustomerSegmentationGraphs

        return super().get_validation_class()
