from flask import request

from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from base.middlewares import attach_carriers_colors
from booking_trends.forms import BookingCountryOptions, BookingCurve, BookingTrends
from booking_trends.service import BookingTrendsService
from utils.funcs import create_error_response

booking_service = BookingTrendsService()


class BookingTrendsController(BaseController):

    @attach_carriers_colors()
    def get(self, endpoint: str):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_BOOKING_CURVE.value:
            return booking_service.get_booking_curve(form)

        if endpoint == ProtectedRoutes.GET_BOOKING_COUNTRY_HOLIDAY_OPTIONS.value:
            return booking_service.get_booking_country_holiday_options(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == ProtectedRoutes.GET_BOOKING_CURVE.value:
            return BookingCurve

        if endpoint == ProtectedRoutes.GET_BOOKING_COUNTRY_HOLIDAY_OPTIONS.value:
            return BookingCountryOptions

        return super().get_validation_class()
