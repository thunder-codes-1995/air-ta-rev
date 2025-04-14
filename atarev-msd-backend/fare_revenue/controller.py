from flask import request

from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from base.middlewares import attach_carriers_colors
from fare_revenue.forms import (
    FareRevenueClassMix,
    FareRevenueTrends,
    MSDBookingVsAverageFares,
    MSDFareRevenueDowRevenue,
    MSDProductRevenueFareTrends,
    MSDRbdElastic,
)
from fare_revenue.service import FareRevenueService
from utils.funcs import create_error_response

service = FareRevenueService


class FareRevenueController(BaseController):

    @attach_carriers_colors()
    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_FARE_TRENDS.value:
            return service.get_fare_trends(form)

        if endpoint == ProtectedRoutes.GET_FARE_BOOKING_HISTOGRAM.value:
            return service.get_fare_booking_histograms(form)

        if endpoint == ProtectedRoutes.GET_FARE_REVENUE_DOWN_REVENUE.value:
            return service.get_fare_dow_revenue(form)

        if endpoint == ProtectedRoutes.GET_RBD_ELASTIC_CITIES.value:
            return service.get_msd_rbd_elastic(form)

        if endpoint == ProtectedRoutes.GET_FARE_REVENUE_CLASS_MIX.value:
            return service.get_fare_revenue_class_mix(form)

        if endpoint == ProtectedRoutes.GET_FARE_REVENUE_TRENDS.value:
            return service.get_fare_revenue_trends(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint: str = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_FARE_TRENDS:
            return MSDProductRevenueFareTrends

        if endpoint == ProtectedRoutes.GET_FARE_BOOKING_HISTOGRAM.value:
            return MSDBookingVsAverageFares

        if endpoint == ProtectedRoutes.GET_FARE_REVENUE_DOWN_REVENUE.value:
            return MSDFareRevenueDowRevenue

        if endpoint == ProtectedRoutes.GET_RBD_ELASTIC_CITIES.value:
            return MSDRbdElastic

        if endpoint == ProtectedRoutes.GET_FARE_REVENUE_CLASS_MIX.value:
            return FareRevenueClassMix

        if endpoint == ProtectedRoutes.GET_FARE_REVENUE_TRENDS.value:
            return FareRevenueTrends

        return super().get_validation_class()
